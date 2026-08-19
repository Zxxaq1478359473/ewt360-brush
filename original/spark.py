#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark.py — EWT360 纯本地刷课脚本（单文件，直连 EWT，不依赖任何自建服务器）

输入一个 EWT token，直连 gateway.ewt360.com / bfe.ewt360.com：发现作业 → 课时列表 →
逐课刷课。全程不碰自建服务器；答案查询/自动提交、卡密、计费、账号系统、
WebSocket 进度推送均不在此脚本（仍留在后端）。

────────────────────────────────────────────────────────────────────────
同步约定（三个来源块，改核心代码时用 git diff --no-index 手动核对）：
  [块1] make_signature()               ← backend/app/core/signer.py    （原样）
  [块2] EwtClient + WAF/限速 infra     ← backend/app/core/ewt_client.py（裁减，见下）
  [块3] run_brush_task() 等刷课核心    ← backend/app/core/brusher.py   （仅改 import）
  复制日期：2026-08-14
  核对命令示例：git diff --no-index backend/app/core/signer.py spark.py
────────────────────────────────────────────────────────────────────────

[块2] 已裁掉的方法（答案查询/自动提交，后端答案功能不进本脚本）：
  _answer_headers、get_answer_report、get_answer_sheet(_info)、submit_answer_one、
  submit_answers、submit_answer_paper、submit_paper_final、self_correct(_one)、
  check_paper_finished、check_correctable、get_question_analysis、init_answer_report、
  confirm_report_submission、get_report_answers、submit_corrected、
  get_paper_download_switch、get_submit_paper_type、get_homework_distribution、
  get_tasks_by_day、_get_oss_credentials、upload_answer_image、check_task_paper(_batch)、
  fetch_school_detail、fetch_class_name。

v1 范围：单 token 顺序刷课（一课接一课），无多课并行。多课并行（batch_executor
动态池）不进 v1——单课竞态爆发已有 ~5x 加速，够用。

用法：
  python spark.py                                     # 交互模式：列作业→列课时→选课→刷
  python spark.py --token <T> --all                   # 刷全部未完成课时
  python spark.py --token <T> --homework-id <id>      # 刷指定作业的未完成课时
  python spark.py --token <T> --lesson-id <id> --course-id <id>  # 刷单课时（作业自动解析）
  可选：--speed 1.5  --waf-backoff 120  --waf-retry 2  --qps 120
  token 从 --token 或环境变量 EWT_TOKEN 读取；进度实时打印到 stdout。
"""

import argparse
import asyncio
import hashlib
import hmac
import logging
import os
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import AsyncGenerator

import httpx

# ======================================================================
# [块1] 签名 make_signature()  ← backend/app/core/signer.py（原样）
# ======================================================================
def make_signature(action: int, duration: int, mstid: str,
                   timestamp_ms: int, secret: str) -> str:
    """HMAC-SHA1 签名 — 完全复刻 MSTPlayer makeSecretKey()

    NOTE: SHA-1 is used here because it is required by the upstream
    EWT360 video player API (MSTPlayer). The signature is used for
    API request authentication, not for password hashing or data
    encryption, so the known SHA-1 collision vulnerabilities are not
    a security concern in this context.
    """
    params = {
        "action": str(action),
        "duration": str(duration),
        "mstid": mstid,
        "signatureMethod": "HMAC-SHA1",
        "signatureVersion": "1.0",
        "timestamp": str(timestamp_ms),
        "version": "2022-08-02",
    }
    sign_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(secret.encode(), sign_str.encode(), hashlib.sha1).hexdigest()


# ======================================================================
# [块2] 客户端 EwtClient + WafCaptchaBlocked + is_waf_blocked + _TokenBucket
#       ← backend/app/core/ewt_client.py（2026-08-14 复制，裁掉答案查询/自动提交方法）
# ======================================================================

GATEWAY = "https://gateway.ewt360.com"
VIDEO_BIZ_CODE = "1013"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.76"
)

logger = logging.getLogger("spark")


class WafCaptchaBlocked(RuntimeError):
    """EWT 风控/WAF 拦截（JSON 429「安全威胁」或 HTML 滑块）。需冷却或换 IP/账号。

    继承 RuntimeError：让既有 `except RuntimeError → 400` 处理链能优雅接管，
    同时 brusher 的 `except WafCaptchaBlocked` 专属分支仍能精确识别做冷却重试。
    """


def is_waf_blocked(data) -> bool:
    """识别 EWT 网关返回的 JSON 429「安全威胁」风控拦截。

    （2026-08-12）实测：被 WAF 标记的 IP/账号，gateway 返回
    HTTP 429 + {"code":429,"msg":"很抱歉，您的请求可能对网站造成安全威胁..."}。
    """
    if not isinstance(data, dict):
        return False
    return data.get("code") == 429 and "安全威胁" in str(data.get("msg", ""))


# ---- 全局 EWT 网关请求限速器 ----
# 进程级单例，跨所有 EwtClient 实例共享——对治「同 IP 发包过频」触发 EWT WAF。
# 无锁设计：asyncio 单线程协作调度下，check-and-deduct 段不含 await 即天然原子；
# 唯一 await 在令牌不足时的 sleep——并发 waiter 少量重叠不影响「软限速」目标，
# 且完全不受 event loop 绑定（多 loop 测试安全）。
class _TokenBucket:
    def __init__(self) -> None:
        self._rate = 120.0 / 60.0   # tokens/秒（默认 120 req/min，可配置覆盖）
        self._capacity = 120.0
        self._tokens = 0.0
        self._last_ts = 0.0

    def configure(self, per_minute: float) -> None:
        per_minute = max(0.1, float(per_minute))
        self._rate = per_minute / 60.0
        self._capacity = max(1.0, per_minute)

    async def acquire(self) -> None:
        now = time.monotonic()
        if self._last_ts == 0:
            self._last_ts = now
            self._tokens = self._capacity
        else:
            self._tokens = min(self._capacity, self._tokens + (now - self._last_ts) * self._rate)
            self._last_ts = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return
        wait = (1.0 - self._tokens) / max(self._rate, 1e-6)
        await asyncio.sleep(wait)
        self._tokens = 0.0
        self._last_ts = time.monotonic()


_gateway_limiter = _TokenBucket()


def set_gateway_qps_cap(per_minute: float) -> None:
    """由 CLI（--qps）覆盖全局网关 QPS 上限。"""
    _gateway_limiter.configure(per_minute)


class EwtClient:
    def __init__(self, token: str):
        self.token = token
        self.user_id = int(token.split("-")[0])
        self._client = httpx.AsyncClient(timeout=30, verify=True)

    @property
    def _headers(self):
        return {
            "User-Agent": UA,
            "token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://teacher.ewt360.com",
            "Referer": "https://teacher.ewt360.com/",
            "Ewt-Requestsource": "web",
            "Ewt-Contentstyle": "CamelCase",
        }

    @staticmethod
    def _parse(r: httpx.Response) -> dict:
        """解析网关响应；WAF 拦截（HTML 滑块 / JSON 429）统一抛 WafCaptchaBlocked。"""
        try:
            data = r.json()
        except Exception:
            raise WafCaptchaBlocked(f"WAF 滑块验证拦截（HTTP {r.status_code}，非 JSON）")
        if is_waf_blocked(data):
            raise WafCaptchaBlocked(f"EWT 风控拦截: {data.get('msg', '')[:80]}")
        return data

    async def _get(self, path: str, params: dict = None) -> dict:
        await _gateway_limiter.acquire()
        r = await self._client.get(f"{GATEWAY}{path}", params=params or {}, headers=self._headers)
        return self._parse(r)

    async def _post(self, path: str, body: dict = None, headers: dict = None) -> dict:
        h = {**self._headers}
        if headers:
            h.update(headers)
        await _gateway_limiter.acquire()
        r = await self._client.post(f"{GATEWAY}{path}", json=body or {}, headers=h)
        return self._parse(r)

    # ---------- 播放器 ----------

    async def fetch_global_conf(self, biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """返回 {sessionId, secret, clientIp, diffTime}"""
        local_ts = int(time.time() * 1000)
        data = await self._get(
            "/api/videoplayerprod/videoplayer/getPlayerGlobalConf",
            {"videoBizCode": biz_code, "sdkVersion": "3.0.37", "_": local_ts},
        )
        if not data.get("success"):
            raise RuntimeError(f"getPlayerGlobalConf failed: {data}")
        g = data["data"]["globalInfo"]
        return {
            "sessionId": g["sessionId"],
            "secret": g["secret"],
            "clientIp": g["clientIp"],
            "ts": int(g.get("ts", 0)),
            "diffTime": int(g.get("ts", 0)) - local_ts,
        }

    async def fetch_player_token(self, school_id: int, lesson_id: int,
                                 content_type: int = 1) -> str:
        data = await self._post(
            "/api/homeworkprod/player/getPlayerToken",
            {"schoolId": school_id, "lessonId": lesson_id, "type": 1,
             "contentType": content_type, "videoBizCode": VIDEO_BIZ_CODE},
        )
        if not data.get("success"):
            raise RuntimeError(f"getPlayerToken failed: {data}")
        return data["data"]

    # ---------- 作业/任务 ----------

    async def list_homeworks(self, school_id: int) -> list[dict]:
        """获取作业列表（status 1/2/3）。

        ⚠️ 2026-07-31 修复：EWT token 失效/被挤下线时返回 2001106，此前静默 break
        返回空列表，导致前端显示"暂无作业"。现在检测到登录态失效立即抛明确错误，
        让上层提示"Token 已失效"而非误导为无作业。
        """
        seen = set()
        all_hws = []
        for st in [1, 2, 3]:
            page_index = 1
            while True:
                data = await self._post(
                    "/api/homeworkprod/homework/student/getStudentHomeworkInfo",
                    {"schoolId": school_id, "status": st, "pageIndex": page_index, "pageSize": 100},
                )
                if data.get("success"):
                    hws = data["data"]
                    for hw in hws:
                        hid = hw.get("homeworkId")
                        if hid and hid not in seen:
                            seen.add(hid)
                            all_hws.append(hw)
                    # EWT returns list directly; if fewer than pageSize, no more pages
                    if len(hws) < 100:
                        break
                    page_index += 1
                else:
                    code = str(data.get("code", ""))
                    msg = str(data.get("msg", ""))
                    # 登录态失效/被挤下线 → 明确报错，不静默当"无作业"
                    if (code == "2001106" or "2001106" in msg
                            or "登录状态已过期" in msg or "其他地方登录" in msg
                            or "重新登录" in msg):
                        raise RuntimeError(
                            f"EWT Token 已失效（{code} {msg[:40]}），请重新绑定"
                        )
                    break
        return all_hws

    async def get_homework_must_subjects(self, school_id: int, homework_id: int) -> list[int]:
        """获取作业详情里的必学科目清单（学生真实选科）。

        ⚠️ 2026-08-01 新增：e网通用这个清单区分 必学/选学——
        queryMustLearn=1 查清单内科目的必学任务，=2 查清单外科目的选学任务。
        清单来自 /api/homeworkprod/student/homework/task/getStudentHomeworkInfo。
        """
        data = await self._post(
            "/api/homeworkprod/student/homework/task/getStudentHomeworkInfo",
            {"schoolId": school_id, "homeworkId": homework_id},
        )
        if not data.get("success") or not data.get("data"):
            raise RuntimeError(f"getStudentHomeworkInfo failed: {data.get('msg', '')}")
        return data["data"].get("mustLearnSubjectList") or []

    async def list_tasks(self, school_id: int, homework_id: int,
                         subject_id: str | int = None,
                         day_id: str = None,
                         must_learn_subjects: list[int] = None) -> list[dict]:
        """获取作业下的课时任务。

        ⚠️ 2026-08-01 修复：不再写死 mustLearnSubjectList=[1..16]。
        - must_learn_subjects 传学生真实必学科目清单（来自 get_homework_must_subjects）；
          不传则回退 [1..16]。
        - day_id / subject_id 分支同时跑 queryMustLearn=1（必学）和 =2（选学），
          返回的任务自带 mustLearning 字段（1=必学 0=选学）。
        """
        if must_learn_subjects is None:
            must_learn_subjects = list(range(1, 17))

        # 无过滤（无 day_id/subject_id）：保持旧行为，逐科目查必学
        # ⚠️ 2026-08-03 并行化：16 科目改为 asyncio.gather 并发（单科目内部仍分页）。
        if not day_id and not subject_id:
            async def _fetch_subj(subj: int) -> list[dict]:
                subj_tasks: list[dict] = []
                page_index = 1
                while True:
                    body = {
                        "schoolId": school_id,
                        "homeworkId": homework_id,
                        "pageIndex": page_index,
                        "pageSize": 100,
                        "subjectId": subj,
                        "mustLearnSubjectList": [subj],
                        "queryMustLearn": 1,
                    }
                    data = await self._post(
                        "/api/homeworkprod/student/homework/task/pageHomeworkTasks", body)
                    if not data.get("success"):
                        break
                    pkg = data.get("data", {})
                    tasks = pkg.get("data") or pkg.get("list") or pkg.get("records") or []
                    subj_tasks.extend(tasks)
                    total = pkg.get("totalRecords", 0)
                    if len(subj_tasks) >= total or len(tasks) == 0:
                        break
                    page_index += 1
                return subj_tasks

            results = await asyncio.gather(
                *(_fetch_subj(s) for s in range(1, 17)),  # subjects 1-16
                return_exceptions=True,
            )
            all_tasks: list[dict] = []
            for r in results:
                if isinstance(r, BaseException):
                    continue
                all_tasks.extend(r)
            return all_tasks

        all_tasks = []
        for query_must in (1, 2):  # 1=必学 2=选学
            page_index = 1
            mode_count = 0
            while True:
                body = {
                    "schoolId": school_id,
                    "homeworkId": homework_id,
                    "pageIndex": page_index,
                    "pageSize": 100,
                    "mustLearnSubjectList": must_learn_subjects,
                    "queryMustLearn": query_must,
                }
                if day_id:
                    body["dayId"] = day_id
                if subject_id:
                    body["subjectId"] = int(subject_id)
                data = await self._post(
                    "/api/homeworkprod/student/homework/task/pageHomeworkTasks", body)
                if not data.get("success"):
                    break
                pkg = data.get("data", {})
                tasks = pkg.get("data") or pkg.get("list") or pkg.get("records") or []
                all_tasks.extend(tasks)
                mode_count += len(tasks)
                total = pkg.get("totalRecords", 0)
                if mode_count >= total or len(tasks) == 0:
                    break
                page_index += 1

        return all_tasks

    async def get_day_subject_stat(self, school_id: int, homework_id: int,
                                   must_learn_subjects: list[int] = None) -> dict:
        """获取作业的日期分组统计，返回 {dateStat, subjectStat, homeworkStat}"""
        if must_learn_subjects is None:
            must_learn_subjects = list(range(1, 17))
        data = await self._post(
            "/api/homeworkprod/student/homework/task/getStudentHomeworkDaySubjectStat",
            {"schoolId": school_id, "homeworkId": homework_id,
             "mustLearnSubjectList": must_learn_subjects},
        )
        if not data.get("success") or not data.get("data"):
            raise RuntimeError(f"getStudentHomeworkDaySubjectStat failed: {data.get('msg', '')}")
        return data["data"]

    async def list_video_tasks(self, school_id: int, homework_id: int,
                               must_learn_subjects: list[int] = None,
                               include_finished: bool = False,
                               max_concurrency: int = 10) -> list[dict]:
        """按日期分组**并行**拉取作业下全部视频/校本课时（contentType 1/11）。

        ⚠️ 2026-08-03 新增：替代此前"逐日期串行 list_tasks"的慢路径——
        日期间用 asyncio.gather + 信号量限流并发，墙钟从 ~2N×latency 降到 ~2×latency。

        返回统一结构（与后端 admin/brush 组装字段一致）：
        {lessonId, homeworkId, courseId, title, duration, finished, mustLearn, subjectId,
         subjectName, studyDate, dateTimestamp, contentType}
        - include_finished=False（默认）：跳过已完成课时，finished 恒为 False
        - 单日/单科目拉取失败只记日志跳过，不拖垮整批；结果按日期保序、contentId 去重。
        """
        if must_learn_subjects is None:
            must_learn_subjects = list(range(1, 17))

        # 日期分组；拿不到则退回按科目枚举
        try:
            stat = await self.get_day_subject_stat(school_id, homework_id,
                                                   must_learn_subjects=must_learn_subjects)
            date_stat = stat.get("dateStat", [])
        except Exception:
            date_stat = []

        sem = asyncio.Semaphore(max_concurrency)

        def _build(t: dict, ds: dict | None) -> dict | None:
            ct = t.get("contentType", 1)
            if ct not in (1, 11):
                return None
            lid = t.get("contentId")
            if not lid:
                return None
            finished = t.get("finished", False)
            if not include_finished and finished:
                return None
            return {
                "lessonId": lid,
                "homeworkId": homework_id,
                "courseId": t.get("parentContentId"),
                "title": t.get("title", ""),
                "duration": t.get("duration", 0),
                "finished": finished if include_finished else False,
                "mustLearn": t.get("mustLearning") == 1,
                "subjectId": t.get("subjectId") or 0,
                "subjectName": t.get("subjectName", ""),
                "studyDate": f"{ds['month']}-{ds['day']}" if ds else "",
                "dateTimestamp": ds.get("date", 0) if ds else 0,
                "contentType": ct,
            }

        async def _fetch_day(ds: dict) -> list[dict]:
            async with sem:
                subj_tasks = await self.list_tasks(school_id, homework_id, day_id=ds["dateId"],
                                                   must_learn_subjects=must_learn_subjects)
            return [i for i in (_build(t, ds) for t in subj_tasks) if i]

        async def _fetch_subject(subj: int) -> list[dict]:
            async with sem:
                subj_tasks = await self.list_tasks(school_id, homework_id, subject_id=subj,
                                                   must_learn_subjects=must_learn_subjects)
            return [i for i in (_build(t, None) for t in subj_tasks) if i]

        if date_stat:
            results = await asyncio.gather(*(_fetch_day(ds) for ds in date_stat),
                                           return_exceptions=True)
            groups: list = date_stat
        else:
            results = await asyncio.gather(*(_fetch_subject(s) for s in must_learn_subjects),
                                           return_exceptions=True)
            groups = must_learn_subjects

        seen: set = set()
        out: list[dict] = []
        for group, items in zip(groups, results):
            if isinstance(items, BaseException):
                logger.warning("list_video_tasks 单组拉取失败: hw=%s group=%s err=%s",
                               homework_id, group, items)
                continue
            for item in items:
                if item["lessonId"] in seen:
                    continue
                seen.add(item["lessonId"])
                out.append(item)
        return out

    async def get_lesson_info(self, school_id: int, homework_id: int,
                              lesson_id: int, content_type: int = 1) -> dict:
        data = await self._post(
            "/api/homeworkprod/homework/student/getUserHomeworkLessonTaskInfo",
            {"schoolId": school_id, "homeworkId": homework_id,
             "lessonId": lesson_id, "contentType": content_type},
        )
        if not data.get("success") or not data.get("data"):
            raise RuntimeError(f"getUserHomeworkLessonTaskInfo failed: {data}")
        return data["data"]

    async def check_detection_passed(self, school_id: int, homework_id: int,
                                     lesson_id: int) -> bool:
        """检查刷课后 EWT 是否认定课时已完成。

        EWT 的完成判定是 finished=True（对应 taskStatus=30, ratio=1.0），
        seriousCheckResult 是独立的看课检测信号（0/1/2），不改变完成状态。
        因此完成判定以 finished / ratio / percent 为准。

        ⚠️ 分页陷阱：EWT API 默认 pageSize=10，必须循环翻页。
        """
        # 1) 看课检测接口里的 finished / ratio 是 EWT 权威完成信号
        item = await self._query_serious_check_item(school_id, homework_id, lesson_id)
        if item is not None:
            if item.get("finished") is True or (item.get("ratio") or 0) >= 1.0:
                return True
        # 2) 兜底：课时播放进度达 100%（percent>=1.0）视为已完成。
        try:
            info = await self.get_lesson_info(school_id, homework_id, lesson_id)
            if (info.get("percent") or 0) >= 1.0:
                return True
        except Exception:
            logger.warning(
                f"check_detection_passed: get_lesson_info failed lesson={lesson_id} hw={homework_id}"
            )
        return False

    async def _query_serious_check_item(self, school_id: int, homework_id: int,
                                        lesson_id: int) -> dict | None:
        """查询目标课时的看课检测条目（seriousCheckResult/needCheckPoint/finished/ratio），
        翻页遍历。返回 None 表示未找到。

        ⚠️ EWT 限制（2026-07-31 实测）：此端点 pageSize 上限 <50（50 直接 506），
        且 queryMustLearn=True 时必须带 mustLearnSubjectList（否则 506）。
        用错任一参数 → 请求 506 → 返回 None → check_detection_passed 走 percent 兜底。
        """
        page_index = 1
        page_size = 30  # 必须 <50；30 安全且减少翻页次数
        while True:
            data = await self._post(
                "/api/homeworkprod/homework/student/pageUserVideoTaskByCondition",
                {
                    "schoolId": str(school_id),
                    "pageSize": page_size,
                    "missionType": 2,
                    "homeworkIds": [homework_id],
                    "pageIndex": page_index,
                    "queryMustLearn": True,
                    "mustLearnSubjectList": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],
                },
            )
            if not data.get("success") or not data.get("data"):
                logger.warning(
                    f"_query_serious_check_item failed: code={data.get('code')} "
                    f"msg={str(data.get('msg'))[:80]} lesson={lesson_id} hw={homework_id}"
                )
                return None
            items = data["data"].get("data", [])
            for item in items:
                if str(item.get("contentId") or item.get("lessonId")) == str(lesson_id):
                    return item
            total = data["data"].get("totalRecords", 0)
            if len(items) < page_size or page_index * page_size >= total:
                break
            page_index += 1
        return None

    async def report_video_point(self, school_id: int, homework_id: int,
                                   lesson_id: int) -> bool:
        """自动通过看课检测。

        模拟浏览器两步时序：
        1. seriousCheckResult=0 通知服务端检测弹窗已出现（激活检测状态）
        2. seriousCheckResult=2 点击"继续观看"通过检测
        """
        # Step 1: 通知检测弹窗出现
        await self._post(
            "/api/homeworkprod/homework/student/addVideoss",
            {
                "schoolId": school_id,
                "homeworkId": homework_id,
                "lessonId": str(lesson_id),
                "type": 1,
                "interactivePointId": None,
                "platform": 1,
                "seriousCheckResult": 0,
            },
        )
        # 模拟用户反应时间（HAR 中约 5 秒，取 3-5 秒随机）
        await asyncio.sleep(3 + random.random() * 2)
        # Step 2: 模拟点击通过
        data = await self._post(
            "/api/homeworkprod/homework/student/addVideoss",
            {
                "schoolId": school_id,
                "homeworkId": homework_id,
                "lessonId": str(lesson_id),
                "type": 1,
                "interactivePointId": None,
                "platform": 1,
                "seriousCheckResult": 2,
            },
        )
        return data.get("success", False)

    async def pass_serious_check(self, school_id: int, homework_id: int,
                                 lesson_id: int) -> bool:
        """刷课后主动把 seriousCheckResult 置为 2（看课检测通过）。

        背景（2026-08-01）：被风控标记的账号刷完后，若没"过滑块"（addVideoss 两步时序），
        EWT 后台 seriousCheckResult 停在 0，老师端显示"看课检测未通过"。
        本方法在课程已完成（finished）的基础上，额外跑两步 addVideoss 把 scr 拉成 2，
        清理后台标记。**只做清理，不改变完成判定**。

        返回：最终 seriousCheckResult >= 2（已通过）。scr==1（错过检测点，addVideoss
        事后无效）返回 False，需走 force_rounds 重刷触发。
        """
        item = await self._query_serious_check_item(school_id, homework_id, lesson_id)
        if item is None:
            return False
        scr = item.get("seriousCheckResult")
        try:
            scr = int(scr or 0)
        except (TypeError, ValueError):
            scr = 0
        if scr >= 2:
            return True
        if scr == 1:
            return False  # 错过检测点，需重刷（force_rounds），这里不做
        # scr == 0：两步 addVideoss 通过（模拟浏览器"点击继续观看"过检测）。
        # 重试最多 3 次直到 scr 变 2（服务端 scr 状态刷新可能有延迟）。
        for attempt in range(1, 4):
            try:
                ok = await self.report_video_point(school_id, homework_id, lesson_id)
                if not ok:
                    logger.warning(
                        f"pass_serious_check addVideoss 未成功 hw={homework_id} "
                        f"lesson={lesson_id} attempt={attempt}"
                    )
                    return False
                await asyncio.sleep(2)
                item2 = await self._query_serious_check_item(school_id, homework_id, lesson_id)
                scr2 = int((item2 or {}).get("seriousCheckResult") or 0)
                if scr2 >= 2:
                    return True
                if attempt < 3:
                    await asyncio.sleep(3)
            except Exception as e:
                logger.warning(
                    f"pass_serious_check failed hw={homework_id} lesson={lesson_id} "
                    f"attempt={attempt}: {e}"
                )
                return False
        logger.warning(
            f"pass_serious_check 3 次后仍未置为通过 hw={homework_id} lesson={lesson_id}"
        )
        return False

    # ---------- 互动弹题 ----------

    async def get_external_video_info(self, lesson_id: int, video_token: str,
                                       biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """获取视频外部信息，包含 interactiveInfo（弹题检测时间点）。

        interactiveInfo.interactiveConfigList: [{id, interactiveTimePoint, ...}, ...]
        interactiveInfo.id: interactiveSceneId（submitAnswer 需要）
        """
        data = await self._get(
            "/api/videoplayerprod/videoplayer/getExternalVideoInfo",
            {
                "videoBizCode": biz_code,
                "lessonId": str(lesson_id),
                "videoToken": video_token,
                "sdkVersion": "3.0.37",
            },
        )
        if not data.get("success"):
            raise RuntimeError(f"getExternalVideoInfo failed: {data}")
        return data["data"]

    async def get_interactive_config_detail(self, interactive_config_id: str,
                                              biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """获取弹题详情（题目和选项列表）。

        返回的 data 包含（HAR 实测）:
        - interactiveSceneId (str 雪花ID)
        - question: {id (str), options: ["A","B","C","D"], cate, answers: null}
        """
        data = await self._get(
            "/api/videoplayerprod/videoplayer/getInteractiveConfigDetail",
            {
                "interactiveConfigId": interactive_config_id,
                "videoBizCode": biz_code,
            },
        )
        if not data.get("success"):
            raise RuntimeError(f"getInteractiveConfigDetail failed: {data}")
        return data["data"]

    async def submit_answer(self, interactive_scene_id: str, question_id: str,
                             my_answers: list, total_seconds: int,
                             lesson_id: int, biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """提交弹题答案，解除播放器暂停状态。

        my_answers: 答案列表，如 ["A"] 单选，["A","C"] 多选
        total_seconds: 当前 playTime（秒，非毫秒）
        """
        data = await self._post(
            "/api/videoplayerprod/videoplayer/submitAnswer",
            {
                "interactiveSceneId": interactive_scene_id,
                "questionId": question_id,
                "myAnswers": my_answers,
                "totalSeconds": total_seconds,
                "videoBizCode": biz_code,
                "lessonId": str(lesson_id),
            },
        )
        if not data.get("success"):
            raise RuntimeError(f"submitAnswer failed: {data}")
        return data["data"]

    # ---------- 用户 ----------

    async def fetch_user_info(self) -> dict:
        """校验 token 有效性并获取基本信息"""
        data = await self._get("/api/usercenter/user/baseinfo")
        if not data.get("success"):
            raise RuntimeError(f"Token 校验失败: {data.get('msg', '')}")
        return data["data"]

    async def fetch_school_info(self) -> dict:
        data = await self._get("/api/eteacherproduct/school/getSchoolUserInfo")
        if not data.get("success"):
            raise RuntimeError(f"获取学校信息失败: {data.get('msg', '')}")
        return data["data"]

    async def close(self):
        await self._client.aclose()


# ======================================================================
# [块3] 刷课核心  ← backend/app/core/brusher.py（2026-08-14 复制）
#       仅改两处 import：app.core.signer / app.core.ewt_client → 同文件直接引用。
#       VIDEO_BIZ_CODE / WafCaptchaBlocked / make_signature 已在 [块1][块2] 定义。
# ======================================================================

BFE = "https://bfe.ewt360.com"
SPEED = 2                # 硬上限！speed=2.1 即触发 699001（实测验证 2026-07-19）
BURST_SIZE = 10          # 竞态并发协程数 (10 ~70%成功率，实测 3-6/10 被 credited)
BURST_WAIT = 10          # 爆发间隔 (秒) — bucket refill 约需 ~12s
SCHOOL_VIDEO_BIZ_CODE = "1014"

# ---- WAF 风控缓解配置（模块级，CLI --waf-* 覆盖） ----
# 2026-08-12：EWT 阿里云 WAF 会按 IP/账号做临时风控标记（qrcode 端点、被标记账号
# 一律 429「安全威胁」）。刷课时检测到 WAF 不再立即失败，而是冷却后重试。
waf_probe_enabled = True        # 刷课前探针（检测 IP/账号是否正被标记）
waf_backoff_seconds = 120.0     # WAF 拦截后冷却秒数
waf_retry_count = 2             # 冷却重试上限（超过则失败）
_waf_probe_interval = 60.0      # 全局探针最小间隔（秒）——批量 12 课时不重复探测
_last_waf_probe_at = 0.0


def set_waf_config(probe: bool, backoff_seconds: float, retry_count: int) -> None:
    """由 CLI（--waf-probe/--waf-backoff/--waf-retry）设置 WAF 缓解参数。"""
    global waf_probe_enabled, waf_backoff_seconds, waf_retry_count
    waf_probe_enabled = bool(probe)
    try:
        waf_backoff_seconds = max(10.0, float(backoff_seconds))
    except (TypeError, ValueError):
        waf_backoff_seconds = 120.0
    try:
        waf_retry_count = max(0, int(retry_count))
    except (TypeError, ValueError):
        waf_retry_count = 2


def is_waf_captcha(response) -> bool:
    """检测阿里云 WAF 滑块验证（acw_tc 人机验证）"""
    ct = response.headers.get("Content-Type", "")
    if "text/html" in ct:
        return True
    text = response.text[:200] if hasattr(response, 'text') else ""
    if not text:
        return False
    # WAF captcha 页面特征：包含 acw_tc 或 <script>challenge
    if not text.startswith("{"):
        return True
    return False

# ---- 实测结论（2026-07-19 本地测试 token=166239195） ----
# 1. stay_time=10000ms 最优: speed×stay_time=20000ms=bucket容量，>10000 被服务端内部 cap
# 2. speed>2 触发 699001: 2.1/2.5/2.9 全部被拒（不是≥3！CLAUDE.md 旧记录有误）
# 3. 多 Session 无效: Bucket 按 token 计，不同 session 共享同一 bucket
# 4. ✅ 多 Lesson 并行有效: Bucket 按 (token, lesson_id) 分片！
#    实测 2 lesson 同时刷 → +120s (vs 单 lesson ~60s) → 2× 加速
#    这是 v12 最大的性能提升手段

# 共享异步客户端（懒加载）：连接复用省 TLS 握手。进程级单例，绑定首次使用的
# event loop——生产 uvicorn 单 worker 单 loop 安全；勿在多 loop（asyncio.run 多次）
# 或热重载测试中复用（会抛 RuntimeError）。max_connections=100 是并发软上限（实测
# 12 并行×10 爆发可容忍，相比旧版全局 10 线程节流是刻意放宽）。
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=30,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
        )
    return _client


@dataclass
class BrushEvent:
    type: str  # "progress" | "done" | "error" | "waf_blocked"
    round: int = 0
    play_time_ms: int = 0
    percent: float = 0.0
    needed_ms: int = 0
    requests_ok: int = 0
    requests_total: int = 0
    credited_sec: int = 0
    message: str = ""


@dataclass
class QuizTimepoint:
    """弹题检测时间点"""
    config_id: str       # interactiveConfigId（字符串雪花ID）
    timepoint_ms: int    # 弹题触发时间（毫秒）
    resolved: bool = False


async def _concurrent_burst(conf, token, lesson_id, course_id, school_id,
                            biz_code, video_type, n_threads=BURST_SIZE,
                            stay_time=10000, speed=SPEED):
    """异步竞态爆发：asyncio.Event 栅栏同时放行 n_threads 个协程打 bfe。
       竞态条件利用不变——服务端 check-and-deduct 非原子，多数请求滑过限流。
       返回 (ok_count, total_count, waf_count)。"""
    client = _get_client()
    start_evt = asyncio.Event()
    results: list = [None] * n_threads

    async def fire_one(i):
        await start_evt.wait()
        # 整段（含 sig/body 构造）包进 try：单条失败降级为 results[i]=False，
        # 不让构造期异常经 gather 传播拖垮整轮爆发。
        try:
            now = int(time.time() * 1000)
            report_time = now + conf["diffTime"]
            begin_time = conf.get("ts", int(time.time() * 1000))
            event_uuid = f"{uuid.uuid4().hex[:8]}_{i}"

            sig = make_signature(2, stay_time, token, report_time, conf["secret"])

            event_pkg = {
                "lesson_id": str(lesson_id),
                "stay_time": stay_time,
                "media_time": 0,
                "status": 1,
                "begin_time": begin_time,
                "report_time": report_time,
                "point_time_id": 200 + i,
                "point_time": 60000,
                "point_num": 20,
                "video_type": video_type,
                "speed": speed,
                "quality": "高清",
                "action": 2,
                "fallback": 0,
                "uuid": event_uuid,
            }
            if course_id is not None:
                event_pkg["course_id"] = str(course_id)

            body = {
                "CommonPackage": {
                    "userid": int(token.split("-")[0]),
                    "ip": conf["clientIp"],
                    "os": "Windows",
                    "resolution": "1920*1080",
                    "mstid": token,
                    "browser": "Edge",
                    "browser_ver": (
                        "5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.76"
                    ),
                    "playerType": 1,
                    "sdkVersion": "3.0.37",
                    "videoBizCode": biz_code,
                    "memberProvinceCode": "320000",
                    "schoolId": str(school_id),
                    "schoolProvinceCode": "320000",
                },
                "EventPackage": [event_pkg],
                "signature": sig,
                "sn": "ewt_web_video_detail",
                "_": int(time.time() * 1000),
            }

            user_id = token.split("-")[0]
            r = await client.post(
                f"{BFE}/monitor/web/collect/batch",
                params={
                    "TrVideoBizCode": biz_code,
                    "TrFallback": "0",
                    "TrUserId": user_id,
                    "TrLessonId": str(lesson_id),
                    "TrUuId": event_uuid,
                    "sdkVersion": "3.0.37",
                    "_": str(int(time.time() * 1000)),
                },
                headers={
                    "token": token,
                    "x-bfe-session-id": conf["sessionId"],
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=10,
            )
            ct = r.headers.get("Content-Type", "")
            if "text/html" in ct or not r.text.startswith("{"):
                results[i] = "waf"
            else:
                results[i] = r.status_code == 200
        except Exception:
            results[i] = False

    tasks = [asyncio.create_task(fire_one(i)) for i in range(n_threads)]
    await asyncio.sleep(0.03)  # 复刻旧版 0.03s 就绪窗口
    start_evt.set()
    await asyncio.gather(*tasks)

    return (sum(1 for r in results if r is True),
            len(results),
            sum(1 for r in results if r == "waf"))


async def _fire_play(conf, token, lesson_id, course_id, school_id, biz_code,
                     video_type, stay_time_ms, speed, use_burst):
    """发一轮播放进度上报。

    use_burst=True:  竞态爆发（默认，~5x 等效加速，speed=2）。
      stay_time 固定 10000ms（实测最优，更大值被服务端内部 cap）。
    use_burst=False: 官方顺序单发（--speed 倍速路径）——一轮一请求，
      stay_time = 本轮实际等待时长，服务端按
      `credited = speed × min(stay_time, real_elapsed)` 计费，
      speed ∈ [0.5, 2.0]，等效加速恰好 = speed×，无竞态利用。
    返回 (ok_count, total_count, waf_count)。
    """
    if use_burst:
        return await _concurrent_burst(
            conf, token, lesson_id, course_id, school_id,
            biz_code, video_type, BURST_SIZE, 10000, speed,
        )
    try:
        await _report_point(
            conf, token, lesson_id, course_id, school_id, 2, 200, 20,
            stay_time=stay_time_ms, speed=speed, begin_offset_ms=stay_time_ms,
            biz_code=biz_code, video_type=video_type,
        )
        return 1, 1, 0
    except WafCaptchaBlocked:
        return 0, 1, 1


async def run_brush_task(
    ewt_client,       # EwtClient instance
    school_id: int,
    homework_id: int,
    lesson_id: int,
    course_id: int | None,
    token: str,       # EWT raw token string
    content_type: int = 1,  # 1=视频, 11=校本视频
    force_rounds: int = 0,  # 强制至少跑N轮（=1事后恢复用，无视 needed<=0）
    phase_offset_ms: int = 0,  # 首轮爆发相位错峰（批量并行用，0=不错峰）
    speed: float | None = None,  # None=默认竞态爆发；有值=官方顺序单发倍速（clamp 0.5..2.0）
) -> AsyncGenerator[BrushEvent, None]:
    """
    执行一次刷课任务，通过 async generator yield 进度事件。

    v12 策略（2026-07-19 实测优化）:
    - stay_time=10s 最优: speed×stay_time=20s=bucket容量，>10s被服务端内部cap
    - speed=2 硬上限: 2.1即触发699001（实测验证）
    - Bucket 按 (token, lesson_id) 分片 → 多 lesson 并行 = N× 加速
    - 竞态并发: 10 协程 Event 同步发射，利用 check-and-deduct 非原子性
    - 等效 ~5x 单 lesson 加速 + N× 多 lesson 并行

    content_type=11 (校本视频) 的关键差异:
    - videoBizCode = "1014" (非 "1013")
    - video_type = 6 (非 1)
    - EventPackage 不含 course_id 字段

    force_rounds: 用于 =1 事后恢复。课程已100%但 detection 未通过时，
    强制跑 N 轮 burst，每轮 delta=0 触发机制B（addVideoss）。

    speed: 倍速（2026-08-03）。**None（默认）= 竞态爆发不变**；
    **传具体值（0.5..2.0）= 官方顺序单发**——每轮只发 1 个官方 monitor 请求
    （speed×real_elapsed 计费，等效加速=speed×），不利用竞态条件。
    有值时硬 clamp 到 [0.5, 2.0]——2.1 即触发 699001（实测）。
    """
    # speed=None → 默认竞态爆发；有值 → 官方顺序单发
    use_burst = speed is None
    if use_burst:
        speed = SPEED  # 竞态爆发内部仍用 speed=2（历史行为）
    else:
        try:
            speed = float(speed)
        except (TypeError, ValueError):
            speed = SPEED
        speed = max(0.5, min(speed, 2.0))

    is_school_video = content_type == 11
    biz_code = SCHOOL_VIDEO_BIZ_CODE if is_school_video else VIDEO_BIZ_CODE
    video_type = 6 if is_school_video else 1

    try:
        # Step 0: WAF 探针 — 检测 IP / 账号是否正被 EWT 风控标记（JSON 429「安全威胁」）
        # 用任务自身 token 打 baseinfo：被标记的 IP 或账号会在这一跳就暴露，提前失败
        # 不启动爆发，避免整批任务白跑。业务异常（token 失效等）由后续正式流程处理。
        if waf_probe_enabled:
            try:
                await ewt_client.fetch_user_info()
            except WafCaptchaBlocked as _w:
                yield BrushEvent(
                    type="waf_blocked",
                    message=f"EWT 风控拦截（{_w}）——IP 或该账号正被标记，"
                            f"请稍后重试/切换 IP/更换账号",
                )
                return
            except Exception:
                pass

        # Step 1: 获取课时信息
        info = await ewt_client.get_lesson_info(school_id, homework_id, lesson_id, content_type=content_type)
        lesson_time_ms = info.get("lessonTime", 0)
        finish_play_time = info.get("finishPlayTime", int(lesson_time_ms * 0.8))
        initial_play_time = info.get("playTime", 0)
        needed = max(0, finish_play_time - initial_play_time)

        if needed <= 0 and force_rounds <= 0:
            yield BrushEvent(type="done", credited_sec=0)
            return

        # Step 2: 获取会话凭证
        conf = await ewt_client.fetch_global_conf(biz_code=biz_code)
        video_token = await ewt_client.fetch_player_token(school_id, lesson_id, content_type=content_type)

        # Step 2.5: 获取弹题时间点（优雅降级，失败不影响刷课）
        quiz_timepoints: list[QuizTimepoint] = []
        interactive_scene_id: str | None = None
        try:
            video_info = await ewt_client.get_external_video_info(
                lesson_id, video_token, biz_code=biz_code,
            )
            interactive_info = video_info.get("interactiveInfo") or {}
            interactive_scene_id = interactive_info.get("id")  # 字符串雪花ID
            config_list = interactive_info.get("interactiveConfigList") or []
            for cfg in config_list:
                cfg_id = cfg.get("id")
                if not cfg_id:
                    continue
                # interactiveTimePoint 单位是毫秒（实测 358331ms ≈ 6min）
                tpoint_ms = cfg.get("interactiveTimePoint", 0)
                quiz_timepoints.append(QuizTimepoint(
                    config_id=str(cfg_id),
                    timepoint_ms=tpoint_ms,
                ))
        except Exception:
            pass  # 无弹题或 API 异常，继续正常刷课

        # 校本视频不带 course_id
        report_cid = course_id if not is_school_video else None

        # WAF 冷却重试（2026-08-12）：检测到 WAF 不立即失败——冷却 waf_backoff_seconds
        # 后重试，连续 waf_retry_count 次仍被拦才失败（临时风控标记冷却后常可恢复）。
        waf_streak = 0

        async def _waf_cooldown() -> bool:
            """WAF 拦截冷却：返回 True=超限需失败，False=已冷却可继续。"""
            nonlocal waf_streak
            waf_streak += 1
            if waf_streak > waf_retry_count:
                return True
            await asyncio.sleep(waf_backoff_seconds)
            return False

        # Step 3: action=1 (play start)
        try:
            await _report_point(conf, token, lesson_id, report_cid,
                                school_id, 1, 0, 20, 0,
                                biz_code=biz_code, video_type=video_type)
        except WafCaptchaBlocked:
            if await _waf_cooldown():
                yield BrushEvent(
                    type="waf_blocked",
                    message=f"EWT 风控拦截（触发人机验证/安全威胁），同 IP 发包过频，"
                            f"请稍后重试或切换网络",
                )
                return

        total_reqs = 1
        ok_count = 1
        current_play_time = initial_play_time
        stall_count = 0
        round_num = 0

        # Step 4: 竞态爆发循环（stay_time=10s 最优，实测验证 2026-07-19）
        #   Bucket 按 (token, lesson_id) 分片 → 多 lesson 并行可获得 N× 加速
        #   单 lesson 内最优参数: stay_time=10s, speed=2, 10并发, 间隔10s
        #   force_rounds>0 时无视 needed，强制跑够轮数（=1 事后恢复）
        while (needed > 0 or round_num < force_rounds) and stall_count < 3:
            round_num += 1

            # 等待间隔 (bucket refill ~12s)。首轮加 phase_offset_ms 错峰。
            # ⚠️ 2026-08-12：BURST_WAIT 加 ±20% 抖动——规律性请求序列更易被 WAF
            # 频率检测识别为自动化，打散节奏降低「发包过频」触发概率。
            wait_sec = BURST_WAIT * random.uniform(0.8, 1.2) + (phase_offset_ms / 1000 if round_num == 1 else 0)
            await asyncio.sleep(wait_sec)

            # 刷新 session（secret 可能过期）
            try:
                conf = await ewt_client.fetch_global_conf(biz_code=biz_code)
            except WafCaptchaBlocked:
                if await _waf_cooldown():
                    yield BrushEvent(type="waf_blocked", message="EWT 风控拦截，请稍后重试或切换网络")
                    return
                continue
            except Exception:
                pass  # 沿用旧 conf

            # 播放上报: 竞态爆发（默认）或官方顺序单发（--speed 倍速，speed×real_elapsed 计费）
            # stay_time = 本轮实际等待时长 wait_sec，保证 min(stay_time, real_elapsed) 满额
            burst_ok, burst_total, burst_waf = await _fire_play(
                conf, token, lesson_id, report_cid, school_id,
                biz_code, video_type, int(wait_sec * 1000), speed, use_burst,
            )
            total_reqs += burst_total
            ok_count += burst_ok

            # WAF 拦截 → 冷却后继续下一轮（临时风控标记冷却后常可恢复）；连续超限才失败
            if burst_waf > 0:
                if await _waf_cooldown():
                    yield BrushEvent(
                        type="waf_blocked",
                        message=f"EWT 风控拦截（{burst_waf}/{burst_total} 请求被拦截），"
                                f"同 IP 发包过频，请稍后重试或切换网络",
                    )
                    return
                continue

            # 查询进度（WAF 拦截时冷却后重试）
            try:
                info2 = await ewt_client.get_lesson_info(school_id, homework_id, lesson_id, content_type=content_type)
            except WafCaptchaBlocked:
                if await _waf_cooldown():
                    yield BrushEvent(type="waf_blocked", message="EWT 风控拦截，请稍后重试或切换网络")
                    return
                continue
            waf_streak = 0
            delta = info2.get("playTime", 0) - current_play_time
            current_play_time = info2.get("playTime", 0)
            needed = max(0, finish_play_time - current_play_time)
            pct = info2.get("percent", 0)

            yield BrushEvent(
                type="progress", round=round_num,
                play_time_ms=current_play_time, percent=pct,
                needed_ms=needed, requests_ok=ok_count, requests_total=total_reqs,
            )

            # 停滞检测：两层恢复策略
            # 机制A: 弹题检测 → 答题绕过（仅非强制模式）
            # 机制B: 看课检测 → addVideoss (原 reportVideoPoint)
            # 强制模式下跳过机制A，直接走机制B（=1事后恢复需要addVideoss）
            if delta == 0:
                quiz_resolved = False
                # --- 机制A：弹题绕过（强制模式下跳过，弹题对 =1 恢复无意义）---
                if round_num > force_rounds:
                    for qt in quiz_timepoints:
                        if qt.resolved:
                            continue
                        if abs(qt.timepoint_ms - current_play_time) > 5000:
                            continue
                        try:
                            detail = await ewt_client.get_interactive_config_detail(
                                qt.config_id, biz_code=biz_code,
                            )
                            scene_id = detail.get("interactiveSceneId") or interactive_scene_id
                            # HAR 实测: question 是单数对象，非数组
                            question = detail.get("question") or {}
                            qid = question.get("id")
                            options = question.get("options") or []  # ["A","B","C","D"]
                            play_sec = max(1, current_play_time // 1000)

                            if qid and options:
                                # 选第一个选项（HAR 实测 A 选对了，rightStatus=1）
                                my_answers = [str(options[0])]
                                await ewt_client.submit_answer(
                                    str(scene_id), str(qid), my_answers, play_sec,
                                    lesson_id=lesson_id, biz_code=biz_code,
                                )

                            qt.resolved = True
                            quiz_resolved = True

                            # 重试上报验证是否恢复（跟随当前模式：爆发/官方顺序）
                            await asyncio.sleep(2)
                            burst_ok2, burst_total2, _ = await _fire_play(
                                conf, token, lesson_id, report_cid, school_id,
                                biz_code, video_type, 10000, speed, use_burst,
                            )
                            total_reqs += burst_total2
                            ok_count += burst_ok2
                            info3 = await ewt_client.get_lesson_info(
                                school_id, homework_id, lesson_id, content_type=content_type,
                            )
                            delta2 = info3.get("playTime", 0) - current_play_time
                            current_play_time = info3.get("playTime", 0)
                            needed = max(0, finish_play_time - current_play_time)
                            if delta2 > 0:
                                stall_count = 0
                                yield BrushEvent(
                                    type="progress", round=round_num,
                                    play_time_ms=current_play_time, percent=info3.get("percent", 0),
                                    needed_ms=needed, requests_ok=ok_count, requests_total=total_reqs,
                                )
                                continue
                        except Exception:
                            pass  # 弹题绕过失败，回退到机制B

                # --- 机制B：看课检测（"点击继续观看"挑战，算法过挑战） ---
                # delta=0 且非弹题 → 服务端弹了点击检测挑战。算法：addVideoss 两步时序
                # （scr=0 激活 → 3-5s → scr=2 通过），最多重试 3 次，每次用双信号确认：
                #   ① 进度恢复（delta2>0）——最快的通过信号
                #   ② scr >= 2（seriousCheckResult 刷新可能滞后于进度）
                if not quiz_resolved:
                    b_passed = False
                    for b_attempt in range(1, 4):
                        try:
                            await ewt_client.report_video_point(school_id, homework_id, lesson_id)
                            await asyncio.sleep(2)
                            burst_ok2, burst_total2, _ = await _fire_play(
                                conf, token, lesson_id, report_cid, school_id,
                                biz_code, video_type, 10000, speed, use_burst,
                            )
                            total_reqs += burst_total2
                            ok_count += burst_ok2
                            info3 = await ewt_client.get_lesson_info(
                                school_id, homework_id, lesson_id, content_type=content_type,
                            )
                            delta2 = info3.get("playTime", 0) - current_play_time
                            current_play_time = info3.get("playTime", 0)
                            needed = max(0, finish_play_time - current_play_time)
                            if delta2 > 0:
                                b_passed = True
                                break
                            # 进度未恢复：复查 scr 是否已变 2（状态刷新可能滞后）
                            try:
                                item_c = await ewt_client._query_serious_check_item(
                                    school_id, homework_id, lesson_id,
                                )
                                if item_c and int(item_c.get("seriousCheckResult") or 0) >= 2:
                                    b_passed = True
                                    break
                            except Exception:
                                pass
                            if b_attempt < 3:
                                await asyncio.sleep(3)
                        except Exception:
                            break
                    if b_passed:
                        stall_count = 0  # 点击挑战通过，重置停滞计数
                        yield BrushEvent(
                            type="progress", round=round_num,
                            play_time_ms=current_play_time, percent=info3.get("percent", 0),
                            needed_ms=needed, requests_ok=ok_count, requests_total=total_reqs,
                        )
                        continue
                    # 强制轮次中 delta=0 是预期行为（已100%），不计入停滞
                    if round_num <= force_rounds:
                        pass
                    else:
                        stall_count += 1
            else:
                stall_count = 0

        # Step 5: action=3 (ended)
        await _report_point(
            conf, token, lesson_id, report_cid,
            school_id, 3, 20, 20,
            stay_time=0, speed=speed, begin_offset_ms=0,
            biz_code=biz_code, video_type=video_type,
        )
        total_reqs += 1
        ok_count += 1

        credited_sec = max(0, (current_play_time - initial_play_time) // 1000)
        yield BrushEvent(
            type="done", credited_sec=credited_sec,
            requests_ok=ok_count, requests_total=total_reqs,
        )

    except Exception as e:
        yield BrushEvent(type="error", message=str(e))


async def _report_point(conf, token, lesson_id, course_id,
                        school_id, action, point_id, point_num,
                        stay_time=0, speed=1, begin_offset_ms=60000,
                        biz_code=VIDEO_BIZ_CODE, video_type=1):
    client = _get_client()
    now = int(time.time() * 1000)
    report_time = now + conf["diffTime"]
    begin_time = report_time - begin_offset_ms
    event_uuid = f"{uuid.uuid4().hex[:8]}_{point_id}"

    sig = make_signature(action, stay_time, token, report_time, conf["secret"])

    event_pkg = {
        "lesson_id": str(lesson_id),
        "stay_time": stay_time,
        "media_time": 0,
        "status": 3 if action == 3 else 1,
        "begin_time": begin_time,
        "report_time": report_time,
        "point_time_id": point_id,
        "point_time": 60000,
        "point_num": point_num,
        "video_type": video_type,
        "speed": speed,
        "quality": "高清",
        "action": action,
        "fallback": 0,
        "uuid": event_uuid,
    }
    if course_id is not None:
        event_pkg["course_id"] = str(course_id)

    body = {
        "CommonPackage": {
            "userid": int(token.split("-")[0]),
            "ip": conf["clientIp"],
            "os": "Windows",
            "resolution": "1920*1080",
            "mstid": token,
            "browser": "Edge",
            "browser_ver": (
                "5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.76"
            ),
            "playerType": 1,
            "sdkVersion": "3.0.37",
            "videoBizCode": biz_code,
            "memberProvinceCode": "320000",
            "schoolId": str(school_id),
            "schoolProvinceCode": "320000",
        },
        "EventPackage": [event_pkg],
        "signature": sig,
        "sn": "ewt_web_video_detail",
        "_": int(time.time() * 1000),
    }

    user_id = token.split("-")[0]
    r = await client.post(
        f"{BFE}/monitor/web/collect/batch",
        params={
            "TrVideoBizCode": biz_code,
            "TrFallback": "0",
            "TrUserId": user_id,
            "TrLessonId": str(lesson_id),
            "TrUuId": event_uuid,
            "sdkVersion": "3.0.37",
            "_": str(int(time.time() * 1000)),
        },
        headers={
            "token": token,
            "x-bfe-session-id": conf["sessionId"],
            "Content-Type": "application/json",
        },
        json=body,
    )
    if is_waf_captcha(r):
        raise WafCaptchaBlocked("WAF 滑块验证拦截 — 同IP发包过频触发人机验证")
    return r.json()


# ======================================================================
# [块4] CLI — 任务发现 + 交互式 + 主入口（本文件原创）
# ======================================================================

EWT_ERROR_MAP: dict[str, str] = {
    "506": "当前账号无权限播放",
    "2001106": "账号在其他地方登录",
}


def _translate_error(msg: str) -> str:
    """翻译已知 EWT 错误码为用户友好中文。"""
    if not msg:
        return msg
    for code, friendly in EWT_ERROR_MAP.items():
        if code in msg:
            return friendly
    return msg


# EWT 登录态失效的权威特征（与 ewt_client.py list_homeworks 的检测一致）。
# 注意：不能用宽泛的 "token" 子串——"getPlayerToken failed: ..." 也含 "token"
# 但那是课时播放 token 获取失败，与账号登录态无关，误判会让整批刷课被当成
# "Token 已失效" 中止（2026-08-01 线上问题）。
_TOKEN_INVALID_MARKERS = (
    "2001106",          # EWT 登录态失效/被挤下线
    "登录状态已过期",
    "其他地方登录",
    "账号在其他地方登录",  # _translate_error 对 2001106 的翻译产物
    "重新登录",
    "EWT Token 已失效",
    "Token 不存在或已失效",
    "Token 校验失败",
)


def _is_token_invalid(msg: str) -> bool:
    """判断错误消息是否代表 EWT 账号登录态失效（而非课时级 token 失败）。"""
    if not msg:
        return False
    return any(m in msg for m in _TOKEN_INVALID_MARKERS)


class TokenInvalidError(RuntimeError):
    """EWT 账号登录态失效——应中止整个运行（后续课时都会同样失败）。"""


def _fmt_ts(ms):
    if not ms:
        return "-"
    return time.strftime("%m-%d %H:%M", time.localtime(ms / 1000))


def _print_homeworks(hws: list[dict]) -> None:
    print(f"\n{'#':<4} {'ID':<12} {'标题':<35} {'布置人':<8} {'开始':<12} {'截止':<12} {'学科'}")
    print("-" * 108)
    for i, hw in enumerate(hws):
        subjs = ", ".join(s["text"] for s in hw.get("subjects", [])[:4])
        teacher = (hw.get("teacherName") or "-")[:8]
        start = _fmt_ts(hw.get("startTime"))
        end = _fmt_ts(hw.get("endTime"))
        print(f"{i + 1:<4} {hw['homeworkId']:<12} {hw['title'][:33]:<35} "
              f"{teacher:<8} {start:<12} {end:<12} {subjs}")


def _print_tasks(tasks: list[dict]) -> None:
    print(f"\n{'#':<4} {'课时ID':<11} {'课程ID':<11} {'类型':<5} {'时长':<7} {'必学':<5} {'标题'}")
    print("-" * 95)
    for i, t in enumerate(tasks):
        mins = (t.get("duration") or 0) // 60
        must = "是" if t.get("mustLearn") else ""
        ct = "校本" if t.get("contentType") == 11 else "视频"
        cid = str(t["courseId"]) if t.get("courseId") else "-"
        print(f"{i + 1:<4} {t['lessonId']:<11} {cid:<11} {ct:<5} "
              f"{mins}min{'':<3} {must:<5} {t['title'][:40]}")


async def _list_homeworks(client: EwtClient, school_id: int) -> list[dict]:
    """拉取作业列表；失败抛 RuntimeError（消息已翻译为用户友好中文）。"""
    try:
        return await client.list_homeworks(school_id)
    except Exception as e:
        raise RuntimeError(_translate_error(str(e))) from e


async def _load_tasks(client: EwtClient, school_id: int, hw_id) -> list[dict] | None:
    """返回作业下未完成视频课时列表（失败返回 None，不抛错）。"""
    try:
        subjects = await client.get_homework_must_subjects(school_id, hw_id)
    except Exception:
        subjects = None  # 拿不到必学清单 → list_video_tasks 回退 [1..16]
    try:
        return await client.list_video_tasks(school_id, hw_id, must_learn_subjects=subjects)
    except Exception as e:
        print(f"  ✗ 获取课时列表失败: {_translate_error(str(e))}")
        return None


async def _find_lesson_task(client: EwtClient, school_id: int, lesson_id,
                            homework_hint=None):
    """按 lesson_id 查找所属作业与课时信息（只读查询）。

    返回 (task, homework_id)；找不到返回 None（可能已刷完 / 不属于任何作业 / token 无权）。
    """
    lesson_id = str(lesson_id)

    def _match(tasks):
        for t in tasks:
            if str(t.get("lessonId")) == lesson_id:
                return t
        return None

    if homework_hint:
        tasks = await _load_tasks(client, school_id, homework_hint)
        if tasks:
            t = _match(tasks)
            if t:
                return t, homework_hint
        return None

    hws = await _list_homeworks(client, school_id)
    for hw in hws:
        hid = hw.get("homeworkId")
        tasks = await _load_tasks(client, school_id, hid)
        if not tasks:
            continue
        t = _match(tasks)
        if t:
            t["homeworkTitle"] = hw.get("title", "")
            return t, hid
        await asyncio.sleep(0.2)  # 轻量节流，避免逐个作业刷屏 EWT
    return None


def _speed_label(speed: float | None) -> str:
    return f"{speed}x（官方顺序单发）" if speed is not None else "竞态爆发（默认）"


async def _brush_once(client: EwtClient, school_id: int, hw_id, lesson_id, course_id,
                      token: str, content_type: int, speed: float | None,
                      force_rounds: int = 0) -> str:
    """执行一次刷课流程，返回状态字符串。

    "ok"              刷课完成且看课检测通过
    "detection_retry" 课时已刷完但检测未通过（需 force_rounds 重刷）
    "error"           业务/网络错误（错误消息已打印、已翻译）
    "waf_blocked"     风控拦截（不重试，提示用户换网络/账号）
    "token_invalid"   EWT 登录态失效（中止整个运行）

    force_rounds>0 时用独立 EwtClient 会话（模拟重播，机制B 触发 addVideoss），
    与后端 _brush_once 行为一致，不影响正常任务会话。
    """
    brush_client = EwtClient(token) if force_rounds > 0 else client
    try:
        async for event in run_brush_task(
            brush_client, school_id, hw_id, lesson_id, course_id, token,
            content_type=content_type, force_rounds=force_rounds, speed=speed,
        ):
            if event.type == "progress":
                print(f"  [进度] 第 {event.round} 轮 | 已播 {event.play_time_ms / 1000:.0f}s"
                      f" | 还需 {event.needed_ms / 1000:.0f}s"
                      f" | 请求 {event.requests_ok}/{event.requests_total}")
            elif event.type == "done":
                print(f"  [完成] 累加 {event.credited_sec}s"
                      f" | 请求 {event.requests_ok}/{event.requests_total}")
                # ---- 机制C：刷完后检查看课检测 seriousCheckResult（finished 维度）----
                try:
                    detection_passed = await brush_client.check_detection_passed(
                        school_id, hw_id, lesson_id)
                except Exception:
                    detection_passed = True  # 查询失败不误判（防 2026-07-31 误判坑）
                if not detection_passed:
                    return "detection_retry"
                # 完成判定通过 → 额外把 scr 拉成 2 清理后台标记（best-effort，失败不阻断）
                try:
                    scr_ok = await brush_client.pass_serious_check(school_id, hw_id, lesson_id)
                except Exception:
                    scr_ok = False
                if scr_ok:
                    print("  [通过] 看课检测已通过")
                else:
                    # 与后端 _recover_serious_check 等价：force_rounds=3 模拟重播后重过
                    print("  ⚠ 课时已完成，但看课检测状态未置为通过，尝试重刷修复…")
                    try:
                        rclient = EwtClient(token)
                        try:
                            async for _ev in run_brush_task(
                                rclient, school_id, hw_id, lesson_id, course_id, token,
                                content_type=content_type, force_rounds=3, speed=speed,
                            ):
                                pass
                            scr_ok2 = await rclient.pass_serious_check(
                                school_id, hw_id, lesson_id)
                        finally:
                            await rclient.close()
                        print("  [通过] 看课检测状态已修复" if scr_ok2
                              else "  ⚠ 看课检测状态仍未能修复，EWT 后台可能显示未通过")
                    except Exception as e:
                        print(f"  ⚠ 重刷修复失败（{_translate_error(str(e))}），"
                              f"best-effort 不阻断任务成功")
                return "ok"
            elif event.type == "waf_blocked":
                print(f"  [WAF拦截] {event.message}")
                return "waf_blocked"
            elif event.type == "error":
                msg = _translate_error(event.message)
                print(f"  [错误] {msg}")
                return "token_invalid" if _is_token_invalid(event.message) else "error"
        return "ok"  # 理论上不可达（run_brush_task 总会 yield 至少一个事件）
    finally:
        if force_rounds > 0:
            await brush_client.close()


async def _brush_lesson(client: EwtClient, school_id: int, hw_id, lesson: dict,
                        token: str, speed: float | None) -> bool:
    """刷单个课时（含机制C重试：检测未通过自动重刷，最多 3 次）。

    返回 True=完成；False=失败/WAF/检测重试耗尽。Token 失效时抛 TokenInvalidError。
    """
    lesson_id = lesson["lessonId"]
    course_id = lesson.get("courseId")
    content_type = lesson.get("contentType", 1)
    title = lesson.get("title") or f"课时 {lesson_id}"
    print(f"\n▶ 开始刷课: {title}")
    print(f"   homework={hw_id} lesson={lesson_id} course={course_id or '-'} "
          f"contentType={content_type} 模式={_speed_label(speed)}")

    DETECTION_MAX_RETRIES = 3
    for attempt in range(1, DETECTION_MAX_RETRIES + 1):
        # 第 2 次起强制 3 轮 burst（模拟重播，机制B 触发 addVideoss 过点击挑战）
        fr = 3 if attempt > 1 else 0
        status = await _brush_once(client, school_id, hw_id, lesson_id, course_id,
                                   token, content_type, speed, force_rounds=fr)
        if status == "ok":
            return True
        if status == "token_invalid":
            raise TokenInvalidError()
        if status == "detection_retry":
            print(f"  ⚠ 看课检测未通过（第 {attempt}/{DETECTION_MAX_RETRIES} 次），"
                  f"{'3s 后自动重刷…' if attempt < DETECTION_MAX_RETRIES else ''}")
            if attempt < DETECTION_MAX_RETRIES:
                await asyncio.sleep(3)
            continue
        return False  # error / waf_blocked — 不重试
    print(f"  ✗ 看课检测未通过，已重试 {DETECTION_MAX_RETRIES} 次，请手动检查")
    return False


async def _brush_all(client: EwtClient, school_id: int, hw_filter=None,
                     speed: float | None = None) -> bool:
    """刷全部未完成课时（hw_filter 指定则只刷该作业）。"""
    hws = await _list_homeworks(client, school_id)
    if not hws:
        print("✗ 当前没有可刷的作业（或 Token 无权访问）")
        return False
    if hw_filter is not None and not any(str(h.get("homeworkId")) == str(hw_filter) for h in hws):
        print(f"✗ 作业 {hw_filter} 不存在或无权访问")
        return False

    all_tasks: list[tuple] = []
    for hw in hws:
        hid = hw.get("homeworkId")
        if hw_filter is not None and str(hid) != str(hw_filter):
            continue
        print(f"查询作业 [{hid}] {hw.get('title', '')[:40]}…")
        tasks = await _load_tasks(client, school_id, hid)
        if not tasks:
            continue
        for t in tasks:
            t["homeworkTitle"] = hw.get("title", "")
            all_tasks.append((hid, t))

    if not all_tasks:
        print("\n没有未完成的课时（可能已全部刷完）")
        return True

    print(f"\n共发现 {len(all_tasks)} 个未完成课时")
    total_duration = sum((t.get("duration") or 0) for _, t in all_tasks)
    print(f"总时长: {total_duration // 60}min{total_duration % 60}s")

    ok_count = 0
    failed: list[dict] = []
    for i, (hid, t) in enumerate(all_tasks, 1):
        ct = "[校本] " if t.get("contentType") == 11 else ""
        print(f"\n{'=' * 62}\n[{i}/{len(all_tasks)}] {ct}"
              f"[{t.get('subjectName', '')}] {t.get('title', '')[:50]}")
        try:
            ok = await _brush_lesson(client, school_id, hid, t, client.token, speed)
        except TokenInvalidError:
            raise
        if ok:
            ok_count += 1
        else:
            failed.append(t)

    print(f"\n{'=' * 62}")
    print(f"处理完成：成功 {ok_count}/{len(all_tasks)}")
    if failed:
        print("失败课时：")
        for t in failed:
            print(f"  - {t.get('title', '')[:50]}")
    return ok_count == len(all_tasks)


async def _interactive(client: EwtClient, school_id: int, speed: float | None) -> bool:
    """交互模式：列作业 → 列课时 → 选课 → 刷。"""
    print("正在获取作业列表…")
    try:
        hws = await _list_homeworks(client, school_id)
    except RuntimeError as e:
        if _is_token_invalid(str(e)):
            raise TokenInvalidError()
        print(f"✗ 获取作业列表失败: {e}")
        return False
    if not hws:
        print("✗ 当前没有可刷的作业（或 Token 无权访问）")
        return False
    _print_homeworks(hws)

    while True:
        choice = input("\n选择作业编号（0 退出）: ").strip()
        if not choice.isdigit():
            continue
        idx = int(choice)
        if idx == 0:
            print("已取消")
            return False
        if not (1 <= idx <= len(hws)):
            print(f"无效选择: {choice}")
            continue
        break

    hw = hws[idx - 1]
    hw_id = hw["homeworkId"]
    print(f"\n已选择: [{hw_id}] {hw['title']}")

    print("正在获取未完成课时…")
    tasks = await _load_tasks(client, school_id, hw_id)
    if not tasks:
        print("✗ 该作业没有未完成的视频课时（或已全部完成）")
        return False
    _print_tasks(tasks)

    while True:
        choice = input("\n选择课时编号（0 全部，q 退出）: ").strip().lower()
        if choice == "q":
            print("已取消")
            return False
        if not choice.isdigit():
            continue
        idx = int(choice)
        if not (1 <= idx <= len(tasks)):
            print(f"无效选择: {choice}")
            continue
        break

    if idx == 0:  # 刷该作业全部未完成课时
        ok_all = True
        for i, t in enumerate(tasks, 1):
            print(f"\n[{i}/{len(tasks)}]")
            try:
                ok = await _brush_lesson(client, school_id, hw_id, t, client.token, speed)
            except TokenInvalidError:
                raise
            ok_all = ok_all and ok
        return ok_all

    t = tasks[idx - 1]
    return await _brush_lesson(client, school_id, hw_id, t, client.token, speed)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="spark.py",
        description="EWT360 纯本地刷课脚本（直连 EWT，不依赖自建服务器）",
        epilog=("token 从 --token 或环境变量 EWT_TOKEN 读取。\n"
                "不传 --speed 走竞态爆发（~5x 等效加速）；传 0.5..2.0 走官方顺序单发倍速。\n"
                "WAF 参数默认对齐后端生产：probe=开、backoff=120s、retry=2 次、qps=120/min。"),
    )
    p.add_argument("--token", help="EWT token（{userId}-{type}-{hex}，如 123456-1-abcdef...）")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--all", action="store_true", help="刷全部未完成课时")
    mode.add_argument("--homework-id", help="只刷指定作业的未完成课时")
    mode.add_argument("--lesson-id", help="只刷单个课时（自动解析作业/课程 ID）")
    p.add_argument("--course-id", help="单课时模式下显式指定课程 ID（缺省自动解析）")
    p.add_argument("--speed", type=float, default=None,
                   help="倍速 0.5..2.0（缺省=竞态爆发）")
    p.add_argument("--waf-probe", action=argparse.BooleanOptionalAction, default=True,
                   help="刷课前 WAF 探针开关（默认开）")
    p.add_argument("--waf-backoff", type=float, default=120.0, help="WAF 拦截冷却秒数（默认 120）")
    p.add_argument("--waf-retry", type=int, default=2, help="WAF 冷却重试上限（默认 2）")
    p.add_argument("--qps", type=float, default=120.0, help="全局网关请求限速 req/min（默认 120）")
    return p


async def _run(args) -> int:
    token = (args.token or os.environ.get("EWT_TOKEN", "")).strip()
    if not token:
        print("✗ 未提供 EWT token。")
        print("  方式1: --token \"123456-1-abcdef...\"")
        print("  方式2: 设置环境变量 EWT_TOKEN")
        return 1
    if not re.match(r"^\d+-(1|2)-[0-9a-fA-F]+$", token):
        print("✗ token 格式不正确，应为 {userId}-{type}-{hex}（如 123456-1-abcdef...）")
        return 1

    # WAF 缓解参数 + 全局限速（默认对齐后端生产，CLI 可覆盖）
    set_waf_config(args.waf_probe, args.waf_backoff, args.waf_retry)
    set_gateway_qps_cap(args.qps)

    # 倍速 clamp（与 brusher 双端 clamp 一致：0.5..2.0，越界忽略回退竞态）
    speed = args.speed
    if speed is not None:
        try:
            speed = float(speed)
        except (TypeError, ValueError):
            speed = None
        if speed is not None and not (0.5 <= speed <= 2.0):
            print(f"⚠ 倍速 {args.speed} 超出 [0.5, 2.0]，已忽略，回退默认竞态模式")
            speed = None

    client = EwtClient(token)
    try:
        # 数据流第一步：fetch_school_info → school_id（token 有效性在这跳验证）
        print("正在获取学校信息…")
        try:
            school_info = await client.fetch_school_info()
            school_id = int(school_info["schoolId"])
        except Exception as e:
            msg = _translate_error(str(e))
            if _is_token_invalid(msg):
                raise TokenInvalidError()
            print(f"✗ 获取学校信息失败: {msg}")
            return 1
        print(f"schoolId: {school_id}")

        if args.lesson_id:
            # ---- 单课时模式：自动解析作业/课程 ID ----
            if args.course_id:
                print(f"使用显式课程 ID: {args.course_id}")
            else:
                print(f"解析课时 {args.lesson_id} 所属作业/课程…")
            lesson_task = await _find_lesson_task(client, school_id, args.lesson_id)
            if not lesson_task:
                print(f"✗ 未找到课时 {args.lesson_id}（可能已刷完、不属于任何作业，"
                      f"或 Token 无权访问）")
                return 1
            lesson, hw_id = lesson_task
            if args.course_id:
                lesson["courseId"] = args.course_id
            return 0 if await _brush_lesson(client, school_id, hw_id, lesson,
                                            token, speed) else 1

        if args.homework_id or args.all:
            ok = await _brush_all(client, school_id,
                                  hw_filter=args.homework_id, speed=speed)
            return 0 if ok else 1

        # ---- 交互模式 ----
        ok = await _interactive(client, school_id, speed)
        return 0 if ok else 1
    except TokenInvalidError:
        print("✗ Token 已失效，请重新扫码获取")
        return 1
    except RuntimeError as e:
        # 任务发现阶段的异常（list_homeworks 已翻译）统一友好处理，不吐 traceback
        msg = str(e)
        if any(k in msg for k in ("风控", "拦截", "安全威胁")):
            print(f"✗ {msg}\n  提示：可能是 IP/账号被 EWT 风控临时标记，可等待冷却或更换网络后重试")
        else:
            print(f"✗ {msg}")
        return 1
    finally:
        await client.close()


def main() -> int:
    args = _build_parser().parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
