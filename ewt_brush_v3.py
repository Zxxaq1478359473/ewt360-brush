#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EWT360 自包含全量刷课脚本（单文件一体化）— [正式版V3 ewt_brush_v3]
============================================================
[V3新增功能]（基于 5 个开源仓库交叉研究，2026-08-20）：
  1. 登录补齐 Web 端签名头（secretId=2: sign/timestamp/autoLogin），
     模拟真实浏览器登录，降低"请完成安全验证"风控触发概率
  2. 任务类型扩展：FM收听(contentType=3)/板报(contentType=5)
     通过 updateMission 一次直写 100%（参考 ECXiaobai/ewt360_tool）
  3. 完成判定后补发 clog.ewt360.com 播放日志（无鉴权端点，4段事件），
     提高后台完成判定率（参考 zjy2fz/ewt-auto-study）
  4. 过课检测参数对齐（参考 luoying2334/EWT360-NEW-Helper 逆向）：
     校本视频(ct=11) lessonId +2000000 偏移；type 按 contentType 计算（视频=1/其他=2）
  5. 完成判定阈值对齐 spark 更新版 [OPTIM]：percent>=0.8（平台真实阈值），
     避免 1.0 过严误判重刷；seriousCheckResult 直查优先、翻页兜底
  本版本即最新正式版，替代 V2，功能最全。
整合：自动登录 + 作业扫描 + N路并行刷课 + 竞态爆发 + WAF冷却重试 + token自动续期。
不依赖 spark.py / ewt_parallel.py，单文件即可运行。
【极限速度测试结论】
  12 路并发 + qps400 = 最优（约7个/分钟，0网络错误）
  10路/qps300 ≈ 2个/分钟；14路/qps500 ERR增多；16路/qps600 连接失败
【用法】
  python3 ewt_brush_v3.py                          # 自动登录并刷全部作业
  python3 ewt_brush_v3.py --hw 10500480            # 只刷指定作业
  python3 ewt_brush_v3.py --token xxx --dry-run    # 用已有token仅扫描
  python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24   # 自定义路数/QPS/爆发
  python3 ewt_brush_v3.py --offset 30 --limit 31   # 分片（多实例并行提速）
【多实例并行（分片提速，实测最优：4实例×12路+burst24+不限速）】
  1. 实例A: python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24 --offset 0 --limit 56
  2. 实例B: python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24 --offset 56 --limit 56 --phase-offset 5000
  3. 实例C: python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24 --offset 112 --limit 56 --phase-offset 10000
  4. 实例D(补刷): python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24 --offset 168
     每个实例可独立设置 --concurrency / --qps / --burst，互不干扰（Bucket 按 token+lesson 分片）。

【token 自动续期】
  刷课中若被挤下线（错误码 2001106）或 token 失效，自动重新登录换新 token
  并重试剩余课时（已完成的自动跳过），最多自动续期 3 次。
"""
import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import sys
import threading
import time
import urllib.parse
import uuid

import httpx
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

# ======================================================================
# [常量]
# ======================================================================
GATEWAY = "https://gateway.ewt360.com"
BFE = "https://bfe.ewt360.com"
VIDEO_BIZ_CODE = "1013"          # 普通视频
SCHOOL_VIDEO_BIZ_CODE = "1014"   # 校本视频
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.76"
)
SPEED = 2                # 硬上限！speed=2.1 即触发 699001（实测验证）
BURST_SIZE = 12          # 单课时竞态爆发并发路数（--burst 可调）
BURST_WAIT = 10          # 爆发间隔（秒）— bucket refill 约需 ~12s
MAX_LOGIN_RETRY = 3      # token 自动续期上限
# WAF 风控缓解配置
WAF_BACKOFF_SECONDS = 120.0   # WAF 拦截后冷却秒数
WAF_RETRY_COUNT = 2           # 冷却重试上限（超过则失败）
# ⚠ 安全：无内置默认账号！账号密码必须由用户显式提供
# （--account/--password 参数，或终端下交互输入）
# token 文件路径：可用环境变量 EWT_TOKEN_FILE 覆盖（手机端 Termux 等无 /root 目录时必用）
TOKEN_FILE = os.environ.get("EWT_TOKEN_FILE", "/root/ewt_token.txt")

logger = logging.getLogger("ewt_brush")


# ======================================================================
# [异常]
# ======================================================================
class WafCaptchaBlocked(RuntimeError):
    """EWT 风控/WAF 拦截（JSON 429「安全威胁」或 HTML 滑块）。"""


class TokenInvalidError(RuntimeError):
    """Token 失效 / 被挤下线（2001106 等）。"""


def is_waf_blocked(data) -> bool:
    """识别 EWT 网关返回的 JSON 429「安全威胁」风控拦截。"""
    if not isinstance(data, dict):
        return False
    return data.get("code") == 429 and "安全威胁" in str(data.get("msg", ""))


def is_waf_captcha(response) -> bool:
    """检测阿里云 WAF 滑块验证（acw_tc 人机验证）。"""
    ct = response.headers.get("Content-Type", "")
    if "text/html" in ct:
        return True
    text = response.text[:200] if hasattr(response, "text") else ""
    if not text:
        return False
    return not text.startswith("{")


_TOKEN_INVALID_MARKERS = (
    "2001106", "其他地方登录", "登录状态已过期", "登录已过期",
    "重新登录", "token 无效", "Token 已失效", "未登录",
)


def _is_token_invalid(msg: str) -> bool:
    if not msg:
        return False
    return any(m in msg for m in _TOKEN_INVALID_MARKERS)


# ======================================================================
# [签名] HMAC-SHA1 — 完全复刻 MSTPlayer makeSecretKey()
# ======================================================================
def make_signature(action: int, duration: int, mstid: str,
                   timestamp_ms: int, secret: str) -> str:
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
# [限速器] 全局 EWT 网关令牌桶（跨所有请求共享）
# ======================================================================
class _TokenBucket:
    def __init__(self) -> None:
        self._rate = 120.0 / 60.0   # tokens/秒
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


# ======================================================================
# [热更新] 运行中动态调整 burst / qps（网页端 /api/config 写配置文件）
# ======================================================================
LIVE_CONFIG_FILE = "/tmp/ewt_live_config.json"


def _load_live_config() -> dict | None:
    """读取网页端热更新配置；无文件/损坏返回 None。"""
    try:
        with open(LIVE_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _apply_live_config() -> None:
    """把热更新配置应用到全局（burst→BURST_SIZE，qps→限流器）。"""
    global BURST_SIZE
    live = _load_live_config()
    if not live:
        return
    try:
        b = int(live.get("burst") or 0)
        if 1 <= b <= 64:
            BURST_SIZE = b
    except (TypeError, ValueError):
        pass
    try:
        q = int(live.get("qps") or 0)
        if q > 0:
            set_gateway_qps_cap(q)
    except (TypeError, ValueError):
        pass


def _live_config_watcher() -> None:
    """后台线程：每 2 秒检查一次热更新配置，运行中即时生效。"""
    while True:
        try:
            _apply_live_config()
        except Exception:
            pass
        time.sleep(2)


threading.Thread(target=_live_config_watcher, daemon=True).start()


# ======================================================================
# [登录] AES 加密密码 → oauth 登录 → token
# ======================================================================
def aes_encrypt(word: str) -> str:
    key = b"20171109124536982017110912453698"
    iv = b"2017110912453698"
    cipher = AES.new(key, AES.MODE_CBC, iv)
    ct = cipher.encrypt(pad(word.encode("utf-8"), AES.block_size))
    return ct.hex().upper()


def _find_token(obj, depth=0) -> str | None:
    """递归查找响应里的 token 字段。"""
    if depth > 4 or obj is None:
        return None
    if isinstance(obj, dict):
        for k in ("token", "accessToken", "access_token", "ticket"):
            v = obj.get(k)
            if isinstance(v, str) and re.match(r"^\d+-(1|2)-[0-9a-fA-F]+$", v):
                return v
        for v in obj.values():
            t = _find_token(v, depth + 1)
            if t:
                return t
    elif isinstance(obj, list):
        for v in obj:
            t = _find_token(v, depth + 1)
            if t:
                return t
    return None


async def login(account: str, password: str, save_file: str = TOKEN_FILE) -> str:
    """登录获取 token 并保存到文件。"""
    enc = aes_encrypt(password)
    url = f"{GATEWAY}/api/authcenter/v2/oauth/login/account"
    ts = int(time.time() * 1000)
    body = {
        "userName": account,
        "password": enc,
        "webVersion": "pc_20250101",
        "platform": 1,          # ⚠️ 必须是 platform（不是 platformType）
        "autoLogin": "true",
    }
    headers = {
        "User-Agent": UA,
        "Content-Type": "application/json",
        "Origin": "https://web.ewt360.com",
        "Referer": "https://web.ewt360.com/",
        # [测试版] 补齐 Web 端签名头（secretId=2）：模拟真实浏览器登录，降低风控识别概率
        "secretid": "2",
        "timestamp": str(ts),
        "sign": hashlib.md5(f"{ts}bdc739ff2dcf".encode()).hexdigest().upper(),
        "platform": "1",
    }
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
        r = await c.post(url, json=body, headers=headers)
        try:
            j = r.json()
        except Exception:
            raise RuntimeError(f"登录失败: 非 JSON 响应 HTTP {r.status_code}")
        if not j.get("success"):
            raise RuntimeError(f"登录失败: {j.get('msg', j)}")
        tok = _find_token(j.get("data"))
        if not tok:
            raise RuntimeError(f"登录成功但未找到 token: {str(j)[:200]}")
    try:
        with open(save_file, "w") as f:
            f.write(tok)
    except Exception:
        pass
    return tok


def load_token_file(path: str = TOKEN_FILE) -> str | None:
    try:
        with open(path) as f:
            tok = f.read().strip()
        if re.match(r"^\d+-(1|2)-[0-9a-fA-F]+$", tok):
            return tok
    except Exception:
        pass
    return None


# ======================================================================
# [客户端] EwtClient — 网关请求封装（带全局限速 + WAF 识别）
# ======================================================================
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
        """解析网关响应；WAF 拦截统一抛 WafCaptchaBlocked。"""
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
        """返回 {sessionId, secret, clientIp, ts, diffTime}"""
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
        """获取作业列表（status 1/2/3）。token 失效抛 TokenInvalidError。"""
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
                    if len(hws) < 100:
                        break
                    page_index += 1
                else:
                    code = str(data.get("code", ""))
                    msg = str(data.get("msg", ""))
                    if _is_token_invalid(f"{code} {msg}"):
                        raise TokenInvalidError(f"EWT Token 已失效（{code} {msg[:40]}）")
                    break
        return all_hws

    async def get_homework_must_subjects(self, school_id: int, homework_id: int) -> list[int]:
        """获取作业详情里的必学科目清单（学生真实选科）。"""
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
        """获取作业下的课时任务（16 科目并发拉取，queryMustLearn=1 必学）。"""
        if must_learn_subjects is None:
            must_learn_subjects = list(range(1, 17))
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
                *(_fetch_subj(s) for s in range(1, 17)),
                return_exceptions=True,
            )
            all_tasks: list[dict] = []
            for r in results:
                if isinstance(r, BaseException):
                    continue
                all_tasks.extend(r)
            return all_tasks
        # 按日期/科目过滤模式（queryMustLearn=1 和 =2 都查）
        all_tasks = []
        for query_must in (1, 2):
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
        """按日期分组**并行**拉取作业下全部视频/校本课时（contentType 1/11）。"""
        if must_learn_subjects is None:
            must_learn_subjects = list(range(1, 17))
        try:
            stat = await self.get_day_subject_stat(school_id, homework_id,
                                                   must_learn_subjects=must_learn_subjects)
            date_stat = stat.get("dateStat", [])
        except Exception:
            date_stat = []
        sem = asyncio.Semaphore(max_concurrency)

        def _build(t: dict, ds: dict | None) -> dict | None:
            ct = t.get("contentType", 1)
            # [测试版] 扩展收集任务型：1=视频 11=校本 3=FM收听 5=板报
            if ct not in (1, 11, 3, 5):
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
        """检查刷课后 EWT 是否认定课时已完成（finished/ratio/percent）。
        [测试版] 对齐 spark 更新版 [OPTIM]：平台达标阈值=80%（finishPercent=0.8），
        用 percent>=0.8 判定，避免 1.0 过严导致已达标课时被误判重刷；
        查询失败不误判（返回 True）。"""
        try:
            info = await self.get_lesson_info(school_id, homework_id, lesson_id)
            if (info.get("percent") or 0) >= 0.8:
                return True
            if info.get("finished") is True:
                return True
        except Exception:
            return True  # 查询失败不误判
        item = await self._query_serious_check_item(school_id, homework_id, lesson_id)
        if item is not None:
            if item.get("finished") is True or (item.get("ratio") or 0) >= 0.8:
                return True
        return False

    async def _query_serious_check_item(self, school_id: int, homework_id: int,
                                        lesson_id: int) -> dict | None:
        """查询目标课时的看课检测条目（seriousCheckResult/finished/ratio），翻页遍历。"""
        page_index = 1
        page_size = 30  # 必须 <50
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
                    "mustLearnSubjectList": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                             11, 12, 13, 14, 15, 16],
                },
            )
            if not data.get("success") or not data.get("data"):
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
                                 lesson_id: int, content_type: int = 1) -> bool:
        """自动通过看课检测（addVideoss 两步时序：scr=0 激活 → scr=2 通过）。
        [测试版] 对齐 luoying2334/EWT360-NEW-Helper 逆向参数（2026.7.30 平台检测）：
          - 校本视频(contentType=11) 的 lessonId 需 +2000000 偏移
          - type: 普通视频=1，其他（校本/FM/板报）=2；互动视频=3"""
        submit_lesson_id = (str(int(lesson_id) + 2000000) if content_type == 11
                            else str(lesson_id))
        submit_type = 1 if content_type == 1 else 2
        await self._post(
            "/api/homeworkprod/homework/student/addVideoss",
            {"schoolId": school_id, "homeworkId": homework_id,
             "lessonId": submit_lesson_id, "type": submit_type,
             "interactivePointId": None, "platform": 1, "seriousCheckResult": 0},
        )
        await asyncio.sleep(3 + random.random() * 2)
        data = await self._post(
            "/api/homeworkprod/homework/student/addVideoss",
            {"schoolId": school_id, "homeworkId": homework_id,
             "lessonId": submit_lesson_id, "type": submit_type,
             "interactivePointId": None, "platform": 1, "seriousCheckResult": 2},
        )
        return data.get("success", False)

    async def pass_serious_check(self, school_id: int, homework_id: int,
                                 lesson_id: int, content_type: int = 1) -> bool:
        """刷课后把 seriousCheckResult 置为 2（看课检测通过）。
        [测试版-降级] 课时进度已达标即可（percent>=0.8），seriousCheckResult 标记
        不影响课时完成。仅做一次 best-effort 置 2，失败不重试、不触发重刷，始终返回 True，
        避免"看课检测未置为通过"导致大量重刷浪费时间和触发风控。"""
        # 最多尝试一次置2（轻量），失败静默
        try:
            info = await self.get_lesson_info(school_id, homework_id, lesson_id)
            scr = info.get("seriousCheckResult")
            if scr is not None:
                scr = int(scr)
                if scr >= 2:
                    return True      # 已通过
                # scr==0/1：做一次 best-effort 置2，失败不回滚
                try:
                    await self.report_video_point(school_id, homework_id, lesson_id,
                                                  content_type=content_type)
                except Exception:
                    pass
                return True          # 无论如何返回 True（不阻断、不重刷）
        except Exception:
            pass
        return True                  # 查询失败也返回 True（不误判、不重刷）

    # ---------- 互动弹题 ----------
    async def get_external_video_info(self, lesson_id: int, video_token: str,
                                      biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """获取视频外部信息，包含 interactiveInfo（弹题检测时间点）。"""
        data = await self._get(
            "/api/videoplayerprod/videoplayer/getExternalVideoInfo",
            {"videoBizCode": biz_code, "lessonId": str(lesson_id),
             "videoToken": video_token, "sdkVersion": "3.0.37"},
        )
        if not data.get("success"):
            raise RuntimeError(f"getExternalVideoInfo failed: {data}")
        return data["data"]

    async def get_interactive_config_detail(self, interactive_config_id: str,
                                            biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """获取弹题详情（题目和选项列表）。"""
        data = await self._get(
            "/api/videoplayerprod/videoplayer/getInteractiveConfigDetail",
            {"interactiveConfigId": interactive_config_id, "videoBizCode": biz_code},
        )
        if not data.get("success"):
            raise RuntimeError(f"getInteractiveConfigDetail failed: {data}")
        return data["data"]

    async def submit_answer(self, interactive_scene_id: str, question_id: str,
                            my_answers: list, total_seconds: int,
                            lesson_id: int, biz_code: str = VIDEO_BIZ_CODE) -> dict:
        """提交弹题答案，解除播放器暂停状态。"""
        data = await self._post(
            "/api/videoplayerprod/videoplayer/submitAnswer",
            {"interactiveSceneId": interactive_scene_id,
             "questionId": question_id, "myAnswers": my_answers,
             "totalSeconds": total_seconds, "videoBizCode": biz_code,
             "lessonId": str(lesson_id)},
        )
        if not data.get("success"):
            raise RuntimeError(f"submitAnswer failed: {data}")
        return data["data"]

    # ---------- 用户 ----------
    async def fetch_user_info(self) -> dict:
        """校验 token 有效性并获取基本信息。"""
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
# [刷课核心] 竞态爆发 + 播放上报 + 看课检测
# ======================================================================
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    """BFE 上报用共享异步客户端（连接复用省 TLS 握手）。"""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=30,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
        )
    return _client


class BrushEvent:
    __slots__ = ("type", "round", "play_time_ms", "percent", "needed_ms",
                 "requests_ok", "requests_total", "credited_sec", "message",
                 "lesson_time_ms")

    def __init__(self, type, round=0, play_time_ms=0, percent=0.0, needed_ms=0,
                 requests_ok=0, requests_total=0, credited_sec=0, message="",
                 lesson_time_ms=0):
        self.type = type          # "progress" | "done" | "error" | "waf_blocked"
        self.round = round
        self.play_time_ms = play_time_ms
        self.percent = percent
        self.needed_ms = needed_ms
        self.requests_ok = requests_ok
        self.requests_total = requests_total
        self.credited_sec = credited_sec
        self.message = message
        self.lesson_time_ms = lesson_time_ms


class QuizTimepoint:
    __slots__ = ("config_id", "timepoint_ms", "resolved")

    def __init__(self, config_id: str, timepoint_ms: int):
        self.config_id = config_id       # interactiveConfigId（字符串雪花ID）
        self.timepoint_ms = timepoint_ms  # 弹题触发时间（毫秒）
        self.resolved = False


async def _concurrent_burst(conf, token, lesson_id, course_id, school_id,
                            biz_code, video_type, n_threads, stay_time=10000,
                            speed=SPEED):
    """异步竞态爆发：asyncio.Event 栅栏同时放行 n_threads 个协程打 bfe。
    竞态条件利用不变——服务端 check-and-deduct 非原子，多数请求滑过限流。
    返回 (ok_count, total_count, waf_count)。"""
    global BURST_SIZE
    # 热更新：始终以全局 BURST_SIZE 为准（watcher 线程每 2 秒同步配置文件）
    n_threads = BURST_SIZE
    client = _get_client()
    start_evt = asyncio.Event()
    results: list = [None] * n_threads

    async def fire_one(i):
        await start_evt.wait()
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
    await asyncio.sleep(0.03)  # 0.03s 就绪窗口
    start_evt.set()
    await asyncio.gather(*tasks)

    return (sum(1 for r in results if r is True),
            len(results),
            sum(1 for r in results if r == "waf"))


async def _fire_play(conf, token, lesson_id, course_id, school_id, biz_code,
                     video_type, stay_time_ms, speed, use_burst, n_threads=BURST_SIZE):
    """发一轮播放进度上报。
    use_burst=True:  竞态爆发（默认，~5x 等效加速，speed=2）。
    use_burst=False: 官方顺序单发（--speed 倍速路径）。
    返回 (ok_count, total_count, waf_count)。"""
    if use_burst:
        return await _concurrent_burst(
            conf, token, lesson_id, course_id, school_id,
            biz_code, video_type, n_threads, 10000, speed,
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


async def run_brush_task(
    ewt_client,       # EwtClient instance
    school_id: int,
    homework_id: int,
    lesson_id: int,
    course_id: int | None,
    token: str,       # EWT raw token string
    content_type: int = 1,  # 1=视频, 11=校本视频
    force_rounds: int = 0,  # 强制至少跑N轮（=1事后恢复用）
    phase_offset_ms: int = 0,  # 首轮爆发相位错峰（批量并行用）
    speed: float | None = None,  # None=默认竞态爆发；有值=官方顺序单发倍速
    n_threads: int = BURST_SIZE,  # 竞态并发路数（可调）
):
    """执行一次刷课任务，通过 async generator yield 进度事件。
    v12 策略:
    - stay_time=10s 最优；speed=2 硬上限（2.1 触发 699001）
    - Bucket 按 (token, lesson_id) 分片 → 多 lesson 并行 = N× 加速
    - 竞态并发: n_threads 协程 Event 同步发射，利用 check-and-deduct 非原子性
    - 等效 ~5x 单 lesson 加速 + N× 多 lesson 并行
    """
    use_burst = speed is None
    if use_burst:
        speed = SPEED
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
            interactive_scene_id = interactive_info.get("id")
            config_list = interactive_info.get("interactiveConfigList") or []
            for cfg in config_list:
                cfg_id = cfg.get("id")
                if not cfg_id:
                    continue
                tpoint_ms = cfg.get("interactiveTimePoint", 0)
                quiz_timepoints.append(QuizTimepoint(
                    config_id=str(cfg_id),
                    timepoint_ms=tpoint_ms,
                ))
        except Exception:
            pass  # 无弹题或 API 异常，继续正常刷课
        # 校本视频不带 course_id
        report_cid = course_id if not is_school_video else None
        # WAF 冷却重试
        waf_streak = 0

        async def _waf_cooldown() -> bool:
            """WAF 拦截冷却：返回 True=超限需失败，False=已冷却可继续。"""
            nonlocal waf_streak
            waf_streak += 1
            if waf_streak > WAF_RETRY_COUNT:
                return True
            await asyncio.sleep(WAF_BACKOFF_SECONDS)
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

        # Step 4: 竞态爆发循环（stay_time=10s 最优，实测验证）
        while (needed > 0 or round_num < force_rounds) and stall_count < 3:
            round_num += 1
            # 等待间隔（bucket refill ~12s）。首轮加 phase_offset_ms 错峰。
            # BURST_WAIT 加 ±20% 抖动——打散节奏降低 WAF 频率识别概率。
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
            # 播放上报
            burst_ok, burst_total, burst_waf = await _fire_play(
                conf, token, lesson_id, report_cid, school_id,
                biz_code, video_type, int(wait_sec * 1000), speed, use_burst,
                n_threads=n_threads,
            )
            total_reqs += burst_total
            ok_count += burst_ok
            # WAF 拦截 → 冷却后继续下一轮
            if burst_waf > 0:
                if await _waf_cooldown():
                    yield BrushEvent(
                        type="waf_blocked",
                        message=f"EWT 风控拦截（{burst_waf}/{burst_total} 请求被拦截），"
                                f"同 IP 发包过频，请稍后重试或切换网络",
                    )
                    return
                continue
            # 查询进度
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
                lesson_time_ms=finish_play_time,
            )
            # 停滞检测：两层恢复策略
            # 机制A: 弹题检测 → 答题绕过（仅非强制模式）
            # 机制B: 看课检测 → addVideoss (原 reportVideoPoint)
            if delta == 0:
                quiz_resolved = False
                # --- 机制A：弹题绕过 ---
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
                            question = detail.get("question") or {}
                            qid = question.get("id")
                            options = question.get("options") or []
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
                            # 重试上报验证是否恢复
                            await asyncio.sleep(2)
                            burst_ok2, burst_total2, _ = await _fire_play(
                                conf, token, lesson_id, report_cid, school_id,
                                biz_code, video_type, 10000, speed, use_burst,
                                n_threads=n_threads,
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
                                    lesson_time_ms=finish_play_time,
                                )
                                continue
                        except Exception:
                            pass  # 弹题绕过失败，回退到机制B
                # --- 机制B：看课检测（"点击继续观看"挑战） ---
                if not quiz_resolved:
                    b_passed = False
                    for b_attempt in range(1, 4):
                        try:
                            await ewt_client.report_video_point(school_id, homework_id, lesson_id,
                                                                content_type=content_type)
                            await asyncio.sleep(2)
                            burst_ok2, burst_total2, _ = await _fire_play(
                                conf, token, lesson_id, report_cid, school_id,
                                biz_code, video_type, 10000, speed, use_burst,
                                n_threads=n_threads,
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
                            # 进度未恢复：复查 scr 是否已变 2
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
                        stall_count = 0
                        yield BrushEvent(
                            type="progress", round=round_num,
                            play_time_ms=current_play_time, percent=info3.get("percent", 0),
                            needed_ms=needed, requests_ok=ok_count, requests_total=total_reqs,
                            lesson_time_ms=finish_play_time,
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
    except WafCaptchaBlocked as e:
        yield BrushEvent(type="waf_blocked", message=str(e))
    except Exception as e:
        msg = str(e)
        if _is_token_invalid(msg):
            yield BrushEvent(type="token_invalid", message=msg)
        else:
            yield BrushEvent(type="error", message=msg)


# ======================================================================
# [主流程] 作业扫描 + 分片 + N路并行 + token自动续期
# ======================================================================
def _translate_error(msg: str) -> str:
    """把网关/HTTP 错误翻译为用户友好中文。"""
    if not msg:
        return msg
    ml = msg.lower()
    if "429" in msg or "安全威胁" in msg or "风控" in msg:
        return "EWT 网关 429「安全威胁」风控拦截（IP 可能被临时标记，稍后重试或换网络）"
    if "2001106" in msg or "其他地方登录" in msg or "登录状态已过期" in msg:
        return "Token 已失效/被挤下线（2001106）"
    if "699001" in msg:
        return "speed 超限（699001，倍速硬上限 2.0）"
    if "timeout" in ml or "timed out" in ml:
        return "网络超时"
    if "connection" in ml and "refused" in ml:
        return "连接被拒绝（可能被限流封禁）"
    return msg


async def _list_homeworks(client: EwtClient, school_id: int) -> list[dict]:
    """拉取作业列表；TokenInvalidError 透传，其余异常翻译。"""
    try:
        return await client.list_homeworks(school_id)
    except TokenInvalidError:
        raise
    except Exception as e:
        raise RuntimeError(_translate_error(str(e))) from e


async def _load_tasks(client: EwtClient, school_id: int, hw_id,
                      include_finished: bool = False) -> list[dict] | None:
    """返回作业下未完成视频课时列表（失败返回 None，不抛错）。
    include_finished=True 时返回全部课时（含已完成，配合 force_rounds 强制重刷）。"""
    try:
        subjects = await client.get_homework_must_subjects(school_id, hw_id)
    except TokenInvalidError:
        raise
    except Exception:
        subjects = None  # 拿不到必学清单 → list_video_tasks 回退 [1..16]
    try:
        return await client.list_video_tasks(school_id, hw_id,
                                             must_learn_subjects=subjects,
                                             include_finished=include_finished)
    except TokenInvalidError:
        raise
    except Exception as e:
        print(f"  ✗ 获取课时列表失败: {_translate_error(str(e))}")
        return None


async def scan_pending_tasks(client: EwtClient, school_id: int,
                             hw_filter=None,
                             include_finished: bool = False) -> list[tuple]:
    """扫描课时，返回 [(homework_id, task), ...]。
    include_finished=True 时含已完成课时（强制重刷模式）。"""
    hws = await _list_homeworks(client, school_id)
    if not hws:
        return []
    if hw_filter is not None and not any(str(h.get("homeworkId")) == str(hw_filter) for h in hws):
        raise RuntimeError(f"作业 {hw_filter} 不存在或无权访问")
    all_tasks: list[tuple] = []
    for hw in hws:
        hid = hw.get("homeworkId")
        if hw_filter is not None and str(hid) != str(hw_filter):
            continue
        print(f"查询作业 [{hid}] {str(hw.get('title', ''))[:40]}…")
        tasks = await _load_tasks(client, school_id, hid, include_finished=include_finished)
        if not tasks:
            continue
        for t in tasks:
            t["homeworkTitle"] = hw.get("title", "")
            t["homeworkId"] = hid
            all_tasks.append((hid, t))
    return all_tasks


async def _run_once(client: EwtClient, school_id: int, hw_id, lesson_id, course_id,
                    token: str, content_type: int, speed, burst_size: int,
                    phase_offset_ms: int, force_rounds: int) -> str:
    """消费 run_brush_task 一次，返回状态: ok | error | waf_blocked | token_invalid。"""
    status = "ok"
    async for ev in run_brush_task(
        client, school_id, hw_id, lesson_id, course_id, token,
        content_type=content_type, force_rounds=force_rounds,
        phase_offset_ms=phase_offset_ms, speed=speed, n_threads=burst_size,
    ):
        if ev.type == "progress":
            print(f"  [进度][{lesson_id}] 第 {ev.round} 轮 | 已播 {ev.play_time_ms / 1000:.0f}s"
                  f"/总长 {ev.lesson_time_ms / 1000:.0f}s"
                  f" | 还需 {ev.needed_ms / 1000:.0f}s"
                  f" | 请求 {ev.requests_ok}/{ev.requests_total}", flush=True)
        elif ev.type == "done":
            print(f"  [完成][{lesson_id}] 累加 {ev.credited_sec}s"
                  f" | 请求 {ev.requests_ok}/{ev.requests_total}", flush=True)
        elif ev.type == "waf_blocked":
            print(f"  [WAF拦截] {ev.message}", flush=True)
            status = "waf_blocked"
        elif ev.type == "token_invalid":
            status = "token_invalid"
        elif ev.type == "error":
            print(f"  [错误] {_translate_error(ev.message)}", flush=True)
            status = "token_invalid" if _is_token_invalid(ev.message) else "error"
    return status


# ======================================================================
# [测试版新增] 任务型直写（FM=3/板报=5）+ clog 播放日志伪造
# ======================================================================
CLOG_URL = "https://clog.ewt360.com"

def _clog_rand_str(n: int = 16) -> str:
    """生成随机 hex 串（避免依赖 string 模块）。"""
    return "".join(random.choice("0123456789abcdef") for _ in range(n))


def _build_clog_payload(lesson_id: str, course_id: str, user_id: str,
                        duration_sec: int) -> tuple:
    """构建 BFE 4 段播放日志（开始/周期/缓冲/结束），与 clog.ewt360.com 无鉴权端点匹配。
    参考 zjy2fz/ewt-auto-study 的 clog_sender.py（2026-08 实测该端点仍无鉴权）。"""
    duration_ms = max(1000, int(duration_sec or 0) * 1000)
    uuid_ = _clog_rand_str()
    dev_id = _clog_rand_str()
    trace_id = _clog_rand_str(32)
    log_id = _clog_rand_str(32)
    begintime = int(time.time() * 1000) - duration_ms - 5000
    brands = [
        {"brand": "Xiaomi", "os": "Android", "os_ver": "14", "model": "Redmi 8", "res": "1417*720"},
        {"brand": "Xiaomi", "os": "Android", "os_ver": "14", "model": "Xiaomi 12", "res": "2400*1080"},
        {"brand": "Huawei", "os": "Android", "os_ver": "12", "model": "Huawei Mate 50", "res": "2700*1224"},
        {"brand": "Apple", "os": "IOS", "os_ver": "17", "model": "iPhone 13 Pro Max", "res": "2778*1284"},
    ]
    dev = random.choice(brands)
    base_extra = {
        "course_id": str(course_id), "trace_id": trace_id,
        "content_type": "video_record", "videoBizCode": 2001,
        "lesson_id": str(lesson_id), "log_index": "42",
        "uuid": uuid_, "fallback": "0", "isonline": "0",
    }

    def _base():
        return {
            "oper_type": "", "version": "10.2.5", "begin_time": begintime,
            "brand": dev["brand"], "carrier": "N/A", "channel": "ewt360",
            "dev_id": dev_id, "imei": "", "imsi": "", "language": "zh",
            "device_model": dev["model"], "sdk_version": "2.0.95-test-rc21",
            "access": "NO_NETWORK", "online": "2", "os": dev["os"],
            "os_version": dev["os_ver"], "point_id": "120",
            "point_time": "60000", "resolution": dev["res"], "speed": "1.0",
            "type": "video_oper", "uploadStatus": -1,
            "url": "com.mistong.ewt360.core.media.video.MediaPlayFragment",
            "userid": str(user_id),
        }

    arr = []
    # Event 1: 主播放开始 (status=2)
    e1 = _base()
    e1["extra"] = base_extra
    e1["log_id"] = log_id
    e1["media_time"] = str(duration_ms)
    e1["stay_time"] = str(duration_ms - 50)
    e1["status"] = "2"
    e1["ts"] = begintime + 100
    arr.append(e1)
    # Event 2: 周期上报 (oper_type=1)
    e2 = _base()
    e2["extra"] = base_extra
    e2["log_id"] = _clog_rand_str(32)
    e2["oper_type"] = "1"
    e2["media_time"] = str(duration_ms)
    e2["stay_time"] = str(duration_ms)
    e2["status"] = ""
    e2["ts"] = begintime + duration_ms // 2
    arr.append(e2)
    # Event 3: 缓冲标记 (status=2, 短停留)
    e3 = _base()
    e3["extra"] = base_extra
    e3["log_id"] = _clog_rand_str(32)
    e3["media_time"] = "0"
    e3["stay_time"] = "350"
    e3["status"] = "2"
    e3["ts"] = begintime + duration_ms + 200
    arr.append(e3)
    # Event 4: 结束标记 (oper_type=6)
    e4 = _base()
    e4["extra"] = base_extra
    e4["log_id"] = _clog_rand_str(32)
    e4["oper_type"] = "6"
    e4["media_time"] = "0"
    e4["stay_time"] = ""
    e4["status"] = ""
    e4["ts"] = begintime + duration_ms + 400
    arr.append(e4)
    body = "sn=moses_ewt_video&log=" + urllib.parse.quote(
        json.dumps(arr, separators=(",", ":"))
    )
    return uuid_, body


async def send_clog_log(user_id: str, lesson_id, course_id, duration_sec: int) -> bool:
    """补发 BFE 播放日志（clog.ewt360.com 无鉴权），提高完成判定率（机制C 补充）。
    失败不影响主流程（best-effort）。"""
    try:
        uuid_, body = _build_clog_payload(str(lesson_id), str(course_id or ""),
                                          str(user_id), int(duration_sec or 0))
        url = f"{CLOG_URL}/?TrUserId={user_id}&TrUuId={uuid_}&TrVideoBizCode=2001&TrFallback=0"
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                url,
                content=body,
                headers={
                    "Host": "clog.ewt360.com",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "okhttp/3.12.0",
                    "sn": "moses_ewt_video",
                },
            )
        return r.status_code == 200
    except Exception:
        return False


async def _mission_direct_complete(client: EwtClient, school_id: int, task: dict) -> bool:
    """[测试版] 任务型直写完成度：FM(3)/板报(5) 等不走播放心跳，
    POST updateMission {schoolId, contentId, contentType, percent:1} 一次直写 100%。
    参考 ECXiaobai/ewt360_tool（2026-08 实测有效）。"""
    lesson_id = task.get("lessonId")
    content_type = task.get("contentType", 3)
    title = task.get("title") or f"课时 {lesson_id}"
    try:
        data = await client._post(
            "/api/homeworkprod/homework/student/updateMission",
            {"schoolId": school_id, "contentId": str(lesson_id),
             "contentType": content_type, "percent": 1},
        )
        ok = bool(data.get("success"))
        print(f"  [直写][{lesson_id}] {str(title)[:40]} "
              f"updateMission(ct={content_type}) → {'✅ 100%' if ok else str(data)[:120]}",
              flush=True)
        return ok
    except Exception as e:
        print(f"  [直写失败][{lesson_id}] {_translate_error(str(e))}", flush=True)
        return False


async def _brush_one(client: EwtClient, school_id: int, hw_id, task: dict,
                     token: str, speed, burst_size: int, phase_offset_ms: int,
                     force_rounds: int = 0) -> bool:
    """刷单个课时（含机制C重试：检测未通过自动重刷，最多 3 次）。
    返回 True=完成；False=失败/WAF/检测重试耗尽。Token 失效抛 TokenInvalidError。"""
    lesson_id = task.get("lessonId")
    course_id = task.get("courseId")
    content_type = task.get("contentType", 1)
    title = task.get("title") or f"课时 {lesson_id}"
    # [测试版] 任务型直写：FM(3)/板报(5) 不走播放心跳，updateMission 一次直写 100%
    if content_type in (3, 5):
        return await _mission_direct_complete(client, school_id, task)
    DETECTION_MAX_RETRIES = 3
    for attempt in range(1, DETECTION_MAX_RETRIES + 1):
        fr = 3 if attempt > 1 else force_rounds  # 第 2 次起强制 3 轮（机制B 触发 addVideoss）
        status = await _run_once(client, school_id, hw_id, lesson_id, course_id,
                                 token, content_type, speed, burst_size,
                                 phase_offset_ms, fr)
        if status == "ok":
            # 机制C：刷完后检查看课检测 seriousCheckResult（finished 维度）
            # [修复] 进度数据有延迟（刷完需几百ms~几秒才刷新到 percent>=0.8），
            # 未通过时先等待+复核，避免误判导致大量重刷浪费时间
            detection_passed = True
            for _check in range(3):   # 最多复核3次
                try:
                    detection_passed = await client.check_detection_passed(
                        school_id, hw_id, lesson_id)
                except Exception:
                    detection_passed = True  # 查询失败不误判
                if detection_passed:
                    break
                await asyncio.sleep(2)   # 等进度刷新后复查
            if not detection_passed:
                print(f"  ⚠ 看课检测未通过（第 {attempt}/{DETECTION_MAX_RETRIES} 次），"
                      f"{'3s 后自动重刷…' if attempt < DETECTION_MAX_RETRIES else ''}")
                if attempt < DETECTION_MAX_RETRIES:
                    await asyncio.sleep(3)
                continue
            # 完成判定通过 → 额外把 scr 拉成 2 清理后台标记（best-effort，失败不重刷）
            try:
                scr_ok = await client.pass_serious_check(school_id, hw_id, lesson_id,
                                                          content_type=content_type)
            except Exception:
                scr_ok = False
            if scr_ok:
                print("  [通过] 看课检测已通过")
            # scr_ok 恒为 True（pass_serious_check 已降级为不重试不重刷），无 else 分支
            # [测试版] 补发 clog 播放日志（无鉴权端点），提高后台完成判定率（best-effort）
            try:
                clog_ok = await send_clog_log(
                    str(getattr(client, "user_id", "0")),
                    lesson_id, course_id or "", task.get("duration") or 0)
                if clog_ok:
                    print("  [clog] 播放日志已补发")
            except Exception:
                pass
            return True
        if status == "token_invalid":
            raise TokenInvalidError()
        if status == "waf_blocked":
            print("  ✗ 风控拦截（不自动重试），请稍后重试或更换网络")
            return False
        return False  # error — 不重试
    print(f"  ✗ 看课检测未通过，已重试 {DETECTION_MAX_RETRIES} 次，请手动检查")
    return False


async def _relogin(account: str, password: str) -> str:
    """自动登录并保存 token，返回新 token。"""
    token = await login(account, password)
    print(f"  ✓ 新 token: {token[:16]}…{token[-8:]}")
    return token
async def run_brush_all(
    token: str,
    account: str,
    password: str,
    hw_filter: str | None = None,
    concurrency: int = 12,
    qps: float = 400.0,
    offset: int = 0,
    limit: int = 0,
    dry_run: bool = False,
    speed: float | None = None,
    force_rounds: int = 0,
    phase_offset_ms: int = 0,
    burst_size: int = BURST_SIZE,
    force_all: bool = False,
) -> int:
    """主流程：扫描 → 分片 → N路并行刷课 → token 自动续期。返回退出码。
    force_all=True：扫描含已完成课时并强制重刷（force_rounds<=0 时默认每课时跑2轮）。"""
    global BURST_SIZE
    if burst_size and burst_size > 0:
        BURST_SIZE = int(burst_size)  # CLI 参数同步全局（热更新 watcher 以此为基准）
    concurrency = max(1, int(concurrency))
    if qps and qps > 0:
        set_gateway_qps_cap(qps)
    # force_all 且未显式指定轮数 → 默认 2 轮（已完成课时 needed<=0，必须 force_rounds>0 才会真跑）
    if force_all and force_rounds <= 0:
        force_rounds = 2
    # 倍速 clamp（越界回退竞态模式）
    if speed is not None:
        try:
            speed = float(speed)
        except (TypeError, ValueError):
            speed = None
        if speed is not None and not (0.5 <= speed <= 2.0):
            print(f"⚠ 倍速 {speed} 超出 [0.5, 2.0]，已忽略，回退默认竞态模式")
            speed = None

    client = EwtClient(token)
    login_retries = 0
    try:
        # ---- 获取 school_id（token 有效性在这跳验证）----
        while True:
            try:
                school_info = await client.fetch_school_info()
                school_id = int(school_info["schoolId"])
                break
            except Exception as e:
                msg = _translate_error(str(e))
                if _is_token_invalid(msg) and login_retries < MAX_LOGIN_RETRY:
                    login_retries += 1
                    print(f"⚠ Token 失效，自动重新登录（{login_retries}/{MAX_LOGIN_RETRY}）…")
                    token = await _relogin(account, password)
                    await client.close()
                    client = EwtClient(token)
                    continue
                print(f"✗ 获取学校信息失败: {msg}")
                return 1
        print(f"schoolId: {school_id}")

        # ---- 扫描课时（force_all 时含已完成）----
        tasks = await scan_pending_tasks(client, school_id, hw_filter,
                                         include_finished=force_all)
        if not tasks:
            print("\n没有未完成的课时（可能已全部刷完）")
            return 0
        if offset or limit:
            tasks = tasks[offset:] if not limit else tasks[offset:offset + limit]
            if not tasks:
                print("✗ 分片范围内没有课时")
                return 0
        if force_all:
            print(f"\n共发现 {len(tasks)} 个课时（含已完成，强制重刷模式，每课时至少 {force_rounds} 轮）")
        else:
            print(f"\n共发现 {len(tasks)} 个未完成课时")
        total_duration = sum((t.get("duration") or 0) for _, t in tasks)
        print(f"总时长: {total_duration // 60}min{total_duration % 60}s")

        # ---- dry-run：仅扫描 ----
        if dry_run:
            for i, (hid, t) in enumerate(tasks, 1):
                ct = "[校本] " if t.get("contentType") == 11 else ""
                print(f"  [{i}/{len(tasks)}] {ct}[{t.get('subjectName', '')}] "
                      f"{str(t.get('title', ''))[:50]} (hw={hid} lesson={t.get('lessonId')}) "
                      f"时长{(t.get('duration') or 0)//60}min{(t.get('duration') or 0)%60}s")
            print("\n[dry-run] 仅扫描，未执行刷课")
            return 0

        # ---- N路并行刷课（分批 gather，每批 concurrency 个）----
        print(f"\n并行路数: {concurrency} | QPS: {qps or '不限'} | "
              f"竞态爆发: {burst_size}路 | 模式: {'倍速' + str(speed) + 'x' if speed else '竞态爆发'}")
        ok_count = 0
        failed: list[dict] = []
        pending = list(tasks)
        while pending:
            batch = pending[:concurrency]
            pending = pending[concurrency:]

            async def _worker(item):
                hid, t = item
                ct = "[校本] " if t.get("contentType") == 11 else ""
                print(f"\n▶ [{t.get('homeworkId')}] {ct}[{t.get('subjectName', '')}] "
                      f"{str(t.get('title', ''))[:50]} "
                      f"[时长{(t.get('duration') or 0)//60}min]", flush=True)
                try:
                    ok = await _brush_one(client, school_id, hid, t, client.token,
                                          speed, burst_size, phase_offset_ms, force_rounds)
                except TokenInvalidError:
                    raise
                return (hid, t, ok)

            try:
                results = await asyncio.gather(*[_worker(item) for item in batch])
            except TokenInvalidError:
                # ---- token 自动续期 ----
                if login_retries >= MAX_LOGIN_RETRY:
                    print(f"✗ Token 再次失效，已达自动续期上限 {MAX_LOGIN_RETRY} 次，中止")
                    break
                login_retries += 1
                print(f"\n⚠ Token 失效/被挤下线，自动重新登录（{login_retries}/{MAX_LOGIN_RETRY}）…")
                token = await _relogin(account, password)
                await client.close()
                client = EwtClient(token)
                try:
                    school_info = await client.fetch_school_info()
                    school_id = int(school_info["schoolId"])
                except Exception as e:
                    print(f"✗ 重新登录后仍无法获取学校信息: {_translate_error(str(e))}")
                    break
                # 重新扫描剩余课时（已完成自动消失），与未刷批次合并
                try:
                    remaining = await scan_pending_tasks(client, school_id, hw_filter,
                                                         include_finished=force_all)
                except Exception as e:
                    print(f"✗ 重新扫描失败: {_translate_error(str(e))}")
                    remaining = []
                pending = remaining + pending
                continue
            for hid, t, ok in results:
                if ok:
                    ok_count += 1
                else:
                    failed.append(t)

        print(f"\n{'=' * 62}")
        print(f"处理完成：成功 {ok_count}/{len(tasks)}")
        if failed:
            print("失败课时：")
            for t in failed:
                print(f"  - {str(t.get('title', ''))[:50]}")
        return 0 if ok_count == len(tasks) else 1
    finally:
        await client.close()


# ======================================================================
# [CLI]
# ======================================================================
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ewt_brush_v3.py",
        description="EWT360 自包含全量刷课脚本【V3正式版】（自动登录 + N路并行 + 竞态爆发 + WAF冷却 + FM/板报直写 + clog补发 + 过课检测优化）",
        epilog=(
            "示例:\n"
            "  python3 ewt_brush_v3.py                          # 自动登录刷全部\n"
            "  python3 ewt_brush_v3.py --hw 10500480            # 只刷指定作业\n"
            "  python3 ewt_brush_v3.py --token xxx --dry-run    # 仅扫描\n"
            "  python3 ewt_brush_v3.py --concurrency 12 --qps 100000 --burst 24\n"
            "  # 多实例分片（推荐 4实例×12路+burst24+不限速，各实例独立调路数/QPS）:\n"
            "  #   实例A: --offset 0 --limit 56 --phase-offset 0\n"
            "  #   实例B: --offset 56 --limit 56 --phase-offset 5000\n"
            "  #   实例C: --offset 112 --limit 56 --phase-offset 10000\n"
            "  #   实例D: --offset 168（补刷）"
        ),
    )
    p.add_argument("--token", help="EWT token（缺省读 TOKEN_FILE，无效则自动登录）")
    p.add_argument("--account", default=None,
                   help="账号（必填，无内置默认；终端下可交互输入或用 --account 指定）")
    p.add_argument("--password", default=None, help="密码（必填，无内置默认；终端下可交互输入或用 --password 指定）")
    p.add_argument("--hw", help="只刷指定作业ID")
    p.add_argument("--concurrency", type=int, default=12, help="并行路数（默认12）")
    p.add_argument("--burst", type=int, default=BURST_SIZE,
                   help="单课时竞态爆发并发数（默认12）")
    p.add_argument("--qps", type=float, default=400.0,
                   help="全局限速 req/min（默认400，0=不限）")
    p.add_argument("--offset", type=int, default=0, help="分片起始下标（多实例用）")
    p.add_argument("--limit", type=int, default=0, help="分片数量，0=全部（多实例用）")
    p.add_argument("--dry-run", action="store_true", help="仅扫描课时不刷课")
    p.add_argument("--speed", type=float, default=None,
                   help="倍速 0.5..2.0（缺省=竞态爆发）")
    p.add_argument("--force-rounds", type=int, default=0,
                   help="强制至少跑N轮（事后修复检测用）")
    p.add_argument("--force-all", action="store_true",
                   help="强制重刷全部课时（含已完成，扫描不过滤；force_rounds<=0 时默认每课时2轮）")
    p.add_argument("--phase-offset", type=int, default=0,
                   help="首轮爆发相位错峰毫秒（多实例并行用）")
    return p


async def _run(args) -> int:
    token = (args.token or "").strip()
    account = (args.account or "").strip()
    password = (args.password or "").strip()
    if not token:
        token = load_token_file()
    if not token or not re.match(r"^\d+-(1|2)-[0-9a-fA-F]+$", token):
        # 账号密码必须显式提供（无内置默认）：--account/--password 参数或终端交互输入
        if (not account or not password) and sys.stdin.isatty():
            try:
                if not account:
                    account = input("账号: ").strip()
                if not password:
                    password = input("密码: ").strip()
            except (EOFError, KeyboardInterrupt):
                pass
        if not account or not password:
            print("✗ 未提供账号/密码：请用 --account 账号 --password 密码，或终端下交互输入")
            return 1
        print("正在登录获取 token…")
        token = await login(account, password)
        if not token:
            print("✗ 自动登录失败，请检查账号密码或网络")
            return 1
        print(f"✓ 登录成功: {token[:16]}…{token[-8:]}")
    else:
        print(f"✓ 使用 token: {token[:16]}…{token[-8:]}")
    return await run_brush_all(
        token, account, password,
        hw_filter=args.hw,
        concurrency=args.concurrency,
        qps=args.qps,
        offset=args.offset,
        limit=args.limit,
        dry_run=args.dry_run,
        speed=args.speed,
        force_rounds=args.force_rounds,
        phase_offset_ms=args.phase_offset,
        burst_size=args.burst,
        force_all=args.force_all,
    )


def main() -> int:
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
