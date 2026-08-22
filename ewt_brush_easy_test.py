#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EWT360 傻瓜式刷课引导器 - 【测试版】（ewt_brush_easy_test.py）
=============================================
一进去只需要回答几个问题，其余全部自动完成：

   ① 输入账号 / 密码
   ② 自动识别任务（扫描并列出所有未完成的课时）
   ③ 刷课配置（实例数 / concurrency / burst / qps）
   ④ 自动登录 → 自动分片 → 多实例后台启动 → 实时监控 → 完成验证

依赖：ewt_brush_v2_test.py 放在同目录。
用法：python3 ewt_brush_easy_test.py
"""
import json
import os
import re
import subprocess
import sys
import time

# ======================================================================
# [路径]
# ======================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BRUSH_SCRIPT = os.path.join(SCRIPT_DIR, "ewt_brush_v2_test.py")
if not os.path.exists(BRUSH_SCRIPT):
    BRUSH_SCRIPT = os.path.join(SCRIPT_DIR, "ewt_brush_v2_test.py")
CONFIG_FILE = os.path.join(SCRIPT_DIR, ".ewt_easy_config.json")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
TOKEN_FILE = "/tmp/ewt_easy_token.txt"   # 多实例共用同一 token，避免互踢

# ======================================================================
# [小工具]
# ======================================================================
def cprint(msg: str = ""):
    print(msg, flush=True)


def ask(question: str, default=None, hint: str = "") -> str:
    """带默认值的交互提问，直接回车用默认值。"""
    if default is not None:
        suffix = f" [默认: {default}]"
    else:
        suffix = ""
    if hint:
        suffix += f"\n    ↳ 提示: {hint}"
    try:
        val = input(f"  {question}{suffix}\n  > ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        val = ""
    if not val and default is not None:
        return str(default)
    return val


def ask_int(question: str, default=None, hint: str = "", lo=None, hi=None) -> int:
    while True:
        val = ask(question, default, hint)
        try:
            n = int(val)
            if lo is not None and n < lo:
                cprint(f"  ⚠ 不能小于 {lo}，重新输入")
                continue
            if hi is not None and n > hi:
                cprint(f"  ⚠ 不能大于 {hi}，重新输入")
                continue
            return n
        except ValueError:
            cprint("  ⚠ 请输入数字")


def load_config() -> dict:
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg: dict):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def run_cmd(cmd: list, timeout=300) -> tuple:
    """执行命令，返回 (exit_code, output)。"""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, encoding="utf-8", errors="replace")
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired as e:
        return -1, (e.stdout or "") + (e.stderr or "")


# ======================================================================
# [第②步] 识别任务：扫描未完成课时
# ======================================================================
def scan_tasks(account: str, password: str, hw: str = "", force_all: bool = False) -> list:
    """调用主脚本 --dry-run 扫描，解析出课时清单。
    force_all=True 时用 --force-all，扫描含已完成的全部课时（用于是否强制重刷）。"""
    cmd = [sys.executable, BRUSH_SCRIPT, "--dry-run",
           "--account", account, "--password", password]
    if force_all:
        cmd += ["--force-all"]           # 扫描含已完成
    if hw:
        cmd += ["--hw", hw]
    env = dict(os.environ)
    env["EWT_TOKEN_FILE"] = TOKEN_FILE
    code, out = run_cmd(cmd, timeout=300)
    # 先过滤掉 Python 异常堆栈（网络偶发波动时主脚本内部已自动重试）
    # 注意：不能按缩进(两空格)过滤，因为 v2 引擎的课时行正是以"  [i/n]"两空格开头
    clean_lines = [l for l in out.splitlines()
                   if not l.startswith("Traceback")
                   and "File \"" not in l and "raise" not in l
                   and "httpx" not in l and "Error" not in l]
    out_clean = "\n".join(clean_lines)
    # 强制模式扫描含已完成时命中"没有未完成"= 连已完成都没有
    if "没有未完成的课时" in out:
        return []
    if code != 0:
        cprint("  ✗ 扫描失败，输出如下：")
        cprint(out[-2000:])
        cprint(f"\n  ✗ 主脚本启动失败（exit={code}）：{BRUSH_SCRIPT}")
        cprint(f"  ✗ 请确认 {os.path.basename(BRUSH_SCRIPT)} 与 {os.path.basename(__file__)} 在同一目录，且文件有读取权限")
        sys.exit(1)
    # 解析课时行（v2 格式： [序号/总数] [科目] 标题 (hw=xxx lesson=xxx)）
    lessons = []
    for line in out_clean.splitlines():
        line = line.strip()
        # 只在包含 "lesson=" 的行才算课时（v2 引擎输出格式固定）
        if "lesson=" not in line:
            continue
        if not line or line.startswith("✓") or line.startswith("schoolId") \
                or line.startswith("查询作业") or "没有未完成" in line \
                or "强制重刷" in line:
            continue
        lessons.append(line)
    return lessons


# ======================================================================
# [第④步] 多实例启动
# ======================================================================
def build_cmd(account: str, password: str, hw: str,
              inst: int, n_inst: int, total: int,
              concurrency: int, burst: int, qps: float,
              force_all: bool = False, force_rounds: int = 0,
              token: str = "") -> list:
    """构造单个实例的命令（自动分片 + 错峰）。force_all=True 时加 --force-all 强制重刷。
    force_rounds>0 时加 --force-rounds N 指定重刷轮数。
    token 非空时用 --token 启动（避免多实例并发登录触发风控）。"""
    cmd = [sys.executable, BRUSH_SCRIPT,
           "--account", account, "--password", password,
           "--concurrency", str(concurrency),
           "--burst", str(burst),
           "--qps", str(qps)]
    if token:
        # 用已登录的 token 启动，跳过子进程登录（避免多实例并发登录风控）
        cmd += ["--token", token]
    if force_all:
        cmd += ["--force-all"]           # 强制重刷全部（含已完成）
    if force_rounds and force_rounds > 0:
        cmd += ["--force-rounds", str(force_rounds)]  # 指定重刷轮数
    if hw:
        cmd += ["--hw", hw]
    chunk = (total + n_inst - 1) // n_inst          # 每片数量（向上取整）
    offset = inst * chunk
    if inst == n_inst - 1:
        cmd += ["--offset", str(offset), "--limit", "0"]   # 最后一片刷到末尾
    else:
        cmd += ["--offset", str(offset), "--limit", str(chunk)]
    if inst > 0:
        cmd += ["--phase-offset", str(inst * 5000)]  # 错峰 5s 递增
    return cmd


def start_instances(account: str, password: str, hw: str, total: int,
                    n_inst: int, concurrency: int, burst: int, qps: float,
                    force_all: bool = False, force_rounds: int = 0,
                    token: str = "") -> list:
    """后台启动 N 个实例，返回 (pid, logfile) 列表。token 非空时传 --token 避免并发登录。"""
    os.makedirs(LOG_DIR, exist_ok=True)
    procs = []
    for i in range(n_inst):
        cmd = build_cmd(account, password, hw, i, n_inst, total,
                        concurrency, burst, qps, force_all, force_rounds, token)
        logf = os.path.join(LOG_DIR, f"inst_{i}.log")
        env = dict(os.environ)
        env["EWT_TOKEN_FILE"] = TOKEN_FILE
        env["PYTHONUNBUFFERED"] = "1"
        with open(logf, "w") as lf:
            p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT,
                                 env=env)
        procs.append((p, logf))
        cprint(f"  ▶ 实例{i + 1} 已启动 PID={p.pid}  日志: {logf}")
        time.sleep(1)   # 错开启动瞬间
    return procs


def count_in_log(logf: str, pattern: str) -> int:
    try:
        with open(logf, encoding="utf-8", errors="replace") as f:
            return sum(1 for line in f if pattern in line)
    except Exception:
        return 0


def find_lesson_label(lessons, lesson_id):
    """在扫描结果中找课时标题做显示标签。"""
    for line in lessons:
        if f"lesson={lesson_id}" in line or f"(hw=" in line and str(lesson_id) in line:
            # 提取标题（去掉编号前缀）
            txt = line.strip()
            for prefix in ("历史-", "语文-", "数学-", "英语-", "物理-", "化学-", "生物-",
                           "政治-", "地理-", "心理-", "生涯-", "综合-", "音乐-", "美术-"):
                if txt.startswith(prefix):
                    return txt[len(prefix):40]
            return txt[:40]
    return f"课时{lesson_id}"


def render_progress_bar(pct: float, width: int = 20) -> str:
    """渲染一个文本进度条：'███████░░░ 35%'"""
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100 * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"{bar} {pct:5.1f}%"


def parse_progress_from_log(logf: str) -> dict:
    """解析实例日志，提取每个课时的当前进度（lesson_id -> 已播/总长/完成）。
    返回 {lesson_id: {'played': float, 'total': float, 'done': bool, 'done_ratio': float}}"""
    out = {}
    try:
        with open(logf, encoding="utf-8", errors="replace") as f:
            for line in f:
                # 新格式：[进度][12345] 第N轮 | 已播 XXXs/总长 YYYs | 还需 ZZZs | 请求 X/Y
                m = re.search(r"\[进度\]\[(\d+)\].*?已播 ([\d.]+)s/总长 ([\d.]+)s", line)
                if m:
                    lid = m.group(1)
                    played = float(m.group(2))
                    total = float(m.group(3))
                    ratio = round(played / (total + 1e-9) * 100, 1) if total > 0 else 0.0
                    out[lid] = {"played": played, "total": total,
                                "done": False, "done_ratio": ratio}
                # 向下兼容旧格式（无总长）
                if not m:
                    m = re.search(r"\[进度\]\[(\d+)\].*?已播 ([\d.]+)s.*?还需 ([\d.]+)s", line)
                    if m:
                        lid = m.group(1)
                        played = float(m.group(2))
                        needed = float(m.group(3))
                        ratio = round(played / (played + needed + 1e-9) * 100, 1)
                        out[lid] = {"played": played, "total": played + needed,
                                    "done": False, "done_ratio": ratio}
                c = re.search(r"\[完成\]\[(\d+)\]", line)
                if c:
                    out[c.group(1)] = {"played": 1e9, "total": 1e9,
                                        "done": True, "done_ratio": 100.0}
    except Exception:
        pass
    return out


def monitor(procs: list, total: int, lessons=None, refresh_interval: float = 3.0):
    """实时监控所有实例，每个课时以进度条形式显示，并显示总进度。
    refresh_interval: 刷新间隔秒数（默认 3 秒，更实时）。
    监控中输入：数字(1-30)调刷新间隔；q 退出监控（后台继续刷）；直接回车刷新一次。"""
    import re as _re
    lessons = lessons or []
    import select as _select
    cprint(f"\n  ===== 实时监控面板（默认每 {refresh_interval} 秒刷新）=====")
    cprint(f"  📌 监控时输入：数字1-30=调刷新间隔(秒) | 回车=立即刷新 | q=退出监控（后台继续刷）")
    start = time.time()
    last_done = 0
    stall = 0
    try:
        while True:
            alive = [p.poll() is None for p, _ in procs]
            if not any(alive):
                break
            done = sum(count_in_log(lf, "[完成]") for _, lf in procs)
            err = sum(count_in_log(lf, "[错误]") for _, lf in procs)
            waf = sum(count_in_log(lf, "WAF") for _, lf in procs)
            el = int(time.time() - start)
            if done == last_done:
                stall += 1
            else:
                stall = 0
                last_done = done
            # ---- 汇总总进度 ----
            pct_total = (done / total * 100) if total > 0 else 100.0
            print(f"\n\x1b[2J\x1b[H", end="", flush=True)  # 清屏重绘
            cprint(f"  ═══════ 实时刷课面板 ═══════")
            cprint(f"  ⏱ {el // 60:02d}m{el % 60:02d}s  运行 {sum(alive)} 实例  "
                   f"错误 {err}  WAF {waf}  进度停滞: {'⚠ 是' if stall >= 3 else '否'}  "
                   f"刷新:{refresh_interval:g}s")
            cprint(f"  📊 总进度: {render_progress_bar(pct_total)}  ({done}/{total} 课时完成)")
            # ---- 每个实例的课时进度条 ----
            for i, (p, lf) in enumerate(procs):
                pg = parse_progress_from_log(lf)
                if not pg:
                    continue
                cprint(f"  ── 实例 {i + 1} ──")
                for lid, info in pg.items():
                    label = find_lesson_label(lessons, lid)
                    if info.get("done"):
                        bar = "█" * 20
                        line = f"    ✅ [{label[:24]:<24}] {bar}  完成"
                    else:
                        pct = info.get("done_ratio", 0.0)
                        bar = render_progress_bar(pct)
                        line = (f"    ▶ [{label[:24]:<24}] {bar}  "
                                f"已播{info.get('played',0):.0f}s/总长{info.get('total',0):.0f}s")
                    cprint(line)
            # 无课时进度时给个提示
            if not any(parse_progress_from_log(lf) for _, lf in procs):
                cprint("    （实例启动中/扫描中…尚未产生课时进度）")
            # ---- 等待下一个刷新周期（可被按键打断立即刷新）----
            wait = refresh_interval
            while wait > 0 and any(p.poll() is None for p, _ in procs):
                try:
                    r, _, _ = _select.select([sys.stdin], [], [], 0.5)
                    if r:
                        key = sys.stdin.readline().strip().lower()
                        if key == "q":
                            cprint(" 已退出监控（后台实例继续刷，日志仍在写）")
                            return
                        elif key.isdigit() and 1 <= int(key) <= 30:
                            refresh_interval = float(int(key))
                            cprint(f"  ✅ 刷新间隔已设为 {refresh_interval:g} 秒")
                            wait = 0  # 立即刷新
                            break
                        else:
                            wait = 0  # 回车立即刷新
                            break
                except Exception:
                    pass
                time.sleep(min(0.5, wait))
                wait -= 0.5
            if wait <= 0:
                continue
    except KeyboardInterrupt:
        print("\n  ⏸ 监控暂停（后台实例继续运行）。回车继续监控 / Esc 退出：")
        try:
            k = input()
            if k.strip().lower() in ("q", "quit", "exit") or k == "\x1b":
                cprint(" 已退出监控（后台实例继续刷，日志仍在写）")
                return
        except Exception:
            return
        # 继续监控
        return monitor(procs, total, lessons, refresh_interval=refresh_interval)
    print()
    el = int(time.time() - start)
    done = sum(count_in_log(lf, "[完成]") for _, lf in procs)
    err = sum(count_in_log(lf, "[错误]") for _, lf in procs)
    waf = sum(count_in_log(lf, "WAF") for _, lf in procs)
    cprint(f"\n  ✅ 全部实例已结束，总耗时 {el // 60}m{el % 60}s")
    cprint(f"     总进度 {done}/{total} 🎉" if done >= total else f"     已完成 {done}/{total}")
    cprint(f"     错误 {err} | WAF {waf}")
    for i, (p, lf) in enumerate(procs):
        cprint(f"     实例{i + 1} 日志: {lf}（退出码 {p.returncode}）")


# ======================================================================
# [主流程]
# ======================================================================
def main():
    cprint("=" * 60)
    cprint("  🚀 EWT360 傻瓜式刷课工具（测试版）v1.0")
    cprint("  只需要回答几个问题，其余全部自动完成")
    cprint("=" * 60)
    if not os.path.exists(BRUSH_SCRIPT):
        cprint(f"\n  ✗ 找不到主脚本，请确认 {BRUSH_SCRIPT} 存在")
        sys.exit(1)

    cfg = load_config()

    # ---------- ① 账号密码 ----------
    cprint("\n【第 1 步】账号 / 密码")
    account = ask("账号", cfg.get("account"), "直接回车沿用上次账号")
    password = ask("密码", cfg.get("password"), "直接回车沿用上次密码")
    if not account or not password:
        cprint("  ✗ 账号密码不能为空")
        sys.exit(1)
    cfg["account"] = account
    cfg["password"] = password
    save_config(cfg)

    # ---------- ② 识别任务 ----------
    cprint("\n【第 2 步】识别任务（自动扫描未完成课时）...")
    cprint("  ⏳ 正在扫描，大约需要 1~2 分钟，请稍候...")
    lessons = scan_tasks(account, password)
    force_all = False
    force_rounds = 0
    if not lessons:
        # 没有未完成课时 → 问是否强制重刷全部（含已完成，用于修复看课检测状态）
        cprint("\n  ✅ 未发现未完成的课时（已刷完）。")
        fask = ask("\n  是否强制重刷全部课时（含已完成，修复看课检测状态）？(y/N)", "N",
                   "y=重刷全部课时，用于看课检测状态异常的课时；n=退出").lower() in ("y", "yes")
        if not fask:
            cprint("  已取消")
            return
        cprint("\n  ⏳ 正在扫描全部课时（含已完成），请稍候...")
        lessons = scan_tasks(account, password, force_all=True)
        if not lessons:
            cprint("\n  ✗ 连已完成课时都没扫到，可能账号无课时或网络问题")
            return
        force_all = True
        force_rounds = ask_int("每课时强制重刷几轮", 2,
                               "重刷轮数越多越彻底（触发看课检测 addVideoss），轮数越高越耗时；2 推荐", lo=1, hi=10)
        cprint(f"  🔁 已切换到【强制重刷全部，每课时 {force_rounds} 轮】模式")
    total = len(lessons)
    cprint(f"\n  🔍 找到 {total} 个课时{'（含已完成，强制重刷）' if force_all else '（未完成）'}：")
    for i, line in enumerate(lessons, 1):
        cprint(f"    [{i:>3}/{total}] {line}")
    cprint(f"  ── 共 {total} 个课时（全部列出，无省略）──")

    hw = ask("\n  要只刷某个作业吗？输入作业ID，或直接回车刷全部", "",
             "作业ID形如 10516876；不填=刷所有作业下的课时")

    # ---------- ③ 刷课配置 ----------
    cprint("\n【第 3 步】刷课配置（直接回车用推荐值）")
    n_inst = ask_int("实例数（并行开的进程数，越多越快）", 1,
                     "1=单实例；2~4=多实例并行，速度成倍提升", lo=1, hi=8)
    concurrency = ask_int("外层路数 concurrency（同时刷几个课时）", 12,
                          "12 最稳最快；14 可试；16 会连接失败", lo=1, hi=14)
    burst = ask_int("内层路数 burst（单课时内同时发多少上报）", 12,
                    "12 默认；24 提速明显；36 更快（极限）", lo=1, hi=64)
    qps = ask("限速 qps（每分钟请求数）", "400",
              "400 稳妥；100000=不限速（实测WAF不易触发，可放心用）")
    try:
        qps = float(qps)
    except ValueError:
        qps = 400.0
    if qps <= 0:
        cprint("  ⚠ 注意：qps=0 并不会不限速（脚本bug），已自动改为 100000")
        qps = 100000.0

    # 确认
    cprint("\n  ── 配置确认 ──")
    cprint(f"     账号: {account}")
    cprint(f"     课时数: {total}")
    cprint(f"     实例数: {n_inst}  |  外层路数: {concurrency}  |  内层路数: {burst}  |  qps: {qps:g}")
    cprint(f"     模式: {'🔁 强制重刷全部（含已完成,每课时' + str(force_rounds) + '轮）' if force_all else '▶ 只刷未完成'}")
    if n_inst > 1:
        chunk = (total + n_inst - 1) // n_inst
        cprint(f"     分片: 每片约 {chunk} 个，实例间错峰 5 秒")
    confirm = ask("\n  确认开始刷课？(Y/n)", "Y")
    if confirm.lower() not in ("y", "yes", ""):
        cprint("  已取消")
        return

    # ---------- ④ 启动 + 监控（失败自动补刷，最多 3 轮） ----------
    # 先主进程登录一次，通过 TOKEN_FILE 获取 token，传给所有子进程（避免多实例并发登录触发风控）
    cprint("\n【第 4 步】登录获取 token…")
    token = ""
    try:
        # 让 v2 引擎写一次 TOKEN_FILE（用 --dry-run 触发登录，不刷课）
        cmd = [sys.executable, BRUSH_SCRIPT,
               "--account", account, "--password", password, "--dry-run",
               "--offset", "0", "--limit", "1"]
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        run_cmd(cmd, timeout=120)
        # 预登录后直接读 TOKEN_FILE（v2 引擎会把 token 写入此文件）
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE) as f:
                token = f.read().strip()
            # 验证 token 格式
            if re.match(r"^\d+-(1|2)-[0-9a-fA-F]+$", token):
                cprint(f"  ✓ 已获取 token: {token[:16]}…{token[-8:]}")
            else:
                cprint(f"  ⚠ token 格式异常: {token[:30]}")
                token = ""
        else:
            cprint("  ⚠ TOKEN_FILE 不存在，子进程将各自登录（可能触发风控）")
    except Exception as e:
        cprint(f"  ⚠ 预登录异常: {e}")

    brush_round = 0
    while True:
        brush_round += 1
        cprint(f"\n【第 4 步】启动刷课（第 {brush_round} 轮）...")
        procs = start_instances(account, password, hw, total,
                                n_inst, concurrency, burst, qps,
                                force_all, force_rounds, token)
        monitor(procs, total, lessons)

        # ---------- ⑤ 完成验证 ----------
        cprint("\n【第 5 步】完成验证（重新扫描确认是否刷完）...")
        lessons2 = scan_tasks(account, password, hw)
        if not lessons2:
            cprint("\n  🎉 全部课时已刷完！验证通过：没有未完成的课时")
            return
        remain = len(lessons2)
        cprint(f"\n  ⚠ 还剩 {remain} 个课时未完成：")
        for line in lessons2[:20]:
            cprint(f"    {line}")
        if remain > 20:
            cprint(f"    ... 共 {remain} 个（其余省略）")
        if brush_round >= 3:
            cprint("\n  ⚠ 已自动补刷 3 轮仍有剩余，建议降低并发/限速后重跑，"
                   "或稍后再试（可能被平台风控临时限制）")
            return
        again = ask(f"\n  是否用相同配置自动补刷剩余 {remain} 个？(Y/n)", "Y",
                    "已完成的课时会自动跳过，不重不漏")
        if again.lower() not in ("y", "yes", ""):
            cprint("  已取消补刷。可随时重新运行本脚本继续（已完成的会自动跳过）")
            return
        total = remain


if __name__ == "__main__":
    main()
