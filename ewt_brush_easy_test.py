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
def scan_tasks(account: str, password: str, hw: str = "") -> list:
    """调用主脚本 --dry-run 扫描，解析出课时清单。"""
    cmd = [sys.executable, BRUSH_SCRIPT, "--dry-run",
           "--account", account, "--password", password]
    if hw:
        cmd += ["--hw", hw]
    env = dict(os.environ)
    env["EWT_TOKEN_FILE"] = TOKEN_FILE
    code, out = run_cmd(cmd, timeout=300)
    # 先过滤掉 Python 异常堆栈（网络偶发波动时主脚本内部已自动重试）
    clean_lines = [l for l in out.splitlines()
                   if not l.startswith("  ") and not l.startswith("Traceback")
                   and "File \"" not in l and "raise" not in l
                   and "httpx" not in l and "Error" not in l]
    out_clean = "\n".join(clean_lines)
    # 成功：没有任何未完成课时
    if "没有未完成的课时" in out:
        return []
    if code != 0:
        cprint("  ✗ 扫描失败，输出如下：")
        cprint(out[-2000:])
        cprint(f"\n  ✗ 主脚本启动失败（exit={code}）：{BRUSH_SCRIPT}")
        cprint(f"  ✗ 请确认 {os.path.basename(BRUSH_SCRIPT)} 与 {os.path.basename(__file__)} 在同一目录，且文件有读取权限")
        sys.exit(1)
    # 解析课时行（v2 格式：科目 标题 [时长] homeworkId=xxx lessonId=xxx）
    lessons = []
    for line in out_clean.splitlines():
        line = line.strip()
        if not line or line.startswith("✓") or line.startswith("schoolId") \
                or line.startswith("查询作业") or "没有未完成" in line:
            continue
        lessons.append(line)
    return lessons


# ======================================================================
# [第④步] 多实例启动
# ======================================================================
def build_cmd(account: str, password: str, hw: str,
              inst: int, n_inst: int, total: int,
              concurrency: int, burst: int, qps: float) -> list:
    """构造单个实例的命令（自动分片 + 错峰）。"""
    cmd = [sys.executable, BRUSH_SCRIPT,
           "--account", account, "--password", password,
           "--concurrency", str(concurrency),
           "--burst", str(burst),
           "--qps", str(qps)]
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
                    n_inst: int, concurrency: int, burst: int, qps: float) -> list:
    """后台启动 N 个实例，返回 (pid, logfile) 列表。"""
    os.makedirs(LOG_DIR, exist_ok=True)
    procs = []
    for i in range(n_inst):
        cmd = build_cmd(account, password, hw, i, n_inst, total,
                        concurrency, burst, qps)
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


def monitor(procs: list, total: int):
    """实时监控所有实例直到全部退出。"""
    cprint("\n  ===== 开始监控（每 20 秒刷新一次，Ctrl+C 可随时查看进度）=====")
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
            st = f"\r  ⏱ {el // 60:02d}m{el % 60:02d}s  已完成 {done}/{total}  "
            st += f"错误 {err}  WAF {waf}  运行中 {sum(alive)} 实例"
            if stall >= 3:
                st += "  ⚠ 进度停滞？"
            print(st + " " * 10, end="", flush=True)
            time.sleep(20)
    except KeyboardInterrupt:
        print("\n  ⏸ 监控暂停（后台实例继续运行）。按回车退出监控：")
        input()
        return
    print()
    el = int(time.time() - start)
    done = sum(count_in_log(lf, "[完成]") for _, lf in procs)
    err = sum(count_in_log(lf, "[错误]") for _, lf in procs)
    waf = sum(count_in_log(lf, "WAF") for _, lf in procs)
    cprint(f"\n  ✅ 全部实例已结束，总耗时 {el // 60}m{el % 60}s")
    cprint(f"     完成 {done}/{total} | 错误 {err} | WAF {waf}")
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
    if not lessons:
        # 没有课时：可能是全刷完了
        cprint("\n  ✅ 没有未完成的课时 —— 该账号已全部刷完！")
        return
    total = len(lessons)
    cprint(f"\n  🔍 找到 {total} 个未完成课时：")
    for i, line in enumerate(lessons[:30], 1):
        cprint(f"    [{i:>2}] {line}")
    if total > 30:
        cprint(f"    ... 共 {total} 个（其余省略）")

    hw = ask("\n  要只刷某个作业吗？输入作业ID，或直接回车刷全部", "",
             "作业ID形如 10516876；不填=刷所有作业下的未完成课时")

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
    if n_inst > 1:
        chunk = (total + n_inst - 1) // n_inst
        cprint(f"     分片: 每片约 {chunk} 个，实例间错峰 5 秒")
    confirm = ask("\n  确认开始刷课？(Y/n)", "Y")
    if confirm.lower() not in ("y", "yes", ""):
        cprint("  已取消")
        return

    # ---------- ④ 启动 + 监控（失败自动补刷，最多 3 轮） ----------
    brush_round = 0
    while True:
        brush_round += 1
        cprint(f"\n【第 4 步】启动刷课（第 {brush_round} 轮）...")
        procs = start_instances(account, password, hw, total,
                                n_inst, concurrency, burst, qps)
        monitor(procs, total)

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
