# 🚀 EWT360 自动刷课工具

> 自包含、多实例并行的 EWT360（升学 e网通）网课平台刷课脚本。
> 自动完成登录、扫描、刷课、监控、验证全流程，单文件即可运行。

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)
[![Deps](https://img.shields.io/badge/deps-2%20packages-orange)](./requirements.txt)

---

## 🙏 致谢 / 原作者说明

本项目源自 [**Ruyi0623/spark_ewt**](https://github.com/Ruyi0623/spark_ewt)（原作者）的 spark 脚本，并在其基础上进行**整合、重写与增强**。

| 项目 | 说明 |
|---|---|
| **原作者** | Ruyi0623（GitHub: [Ruyi0623/spark_ewt](https://github.com/Ruyi0623/spark_ewt)） |
| **原始贡献** | EWT360 播放协议分析、请求签名算法（signer）、核心刷课逻辑 |
| **本仓库改动** | 重写登录（AES+oauth）、作业扫描、并发调度、竞态爆发加速、WAF 冷却重试、token 自动续期、看课检测三重兜底机制，新增多实例分片能力 |

> ⚠️ 原仓库可能已关闭/私有化（访问返回 404），保留链接仅为**作者溯源与致谢**，无侵权意图。若原作者有任何要求，请联系删除。

---

## 📌 简介

EWT360 是网课平台，老师布置的作业中包含大量视频课时，需要逐一看完才算完成。
本工具通过模拟官方播放器的播放上报协议，并利用**竞态爆发**并发技巧，把几十个小时的视频课压缩到几分钟刷完，进度 100% 完成。

**实测**：4 实例并行下，刷 1 小时视频仅需 **32 秒**（效率 RATIO ≈ 0.0089）。

---

## 🧪 测试版（ewt_brush_v2_test.py）

> 基于 5 个开源仓库（zjy2fz/ewt-auto-study、ECXiaobai/ewt360_tool、Klece/killewt、cny123/ewt360-course-tool、hmruu/ewt360）交叉研究后的实验版本，与正式版 `ewt_brush_v2.py` 并存，**不覆盖原版**。

| 新增能力 | 说明 | 参考 |
|---|---|---|
| 🔐 登录签名头 | 补齐 Web 端 `sign=MD5(ts+key)` / `secretid=2` / `timestamp` / `autoLogin`，模拟真实浏览器登录，降低"请完成安全验证"风控概率 | ECXiaobai / cny123 |
| 🎧 FM收听(ct=3) | `updateMission` 一次直写 100%，不走播放心跳 | ECXiaobai/ewt360_tool |
| 📋 板报(ct=5) | 同上，一次直写 100% | ECXiaobai/ewt360_tool |
| 📡 clog 日志补发 | 完成判定后向 `clog.ewt360.com`（无鉴权）补发 4 段播放日志，提高后台完成判定率 | zjy2fz/ewt-auto-study |
| ✅ 过课检测参数对齐 | 校本视频(ct=11) lessonId **+2000000** 偏移；`type` 按 contentType 计算（视频=1/其他=2）——修复看课检测置2失败 | luoying2334/EWT360-NEW-Helper |
| ✅ 判定阈值 0.8 | 完成判定 `percent>=0.8`（平台真实阈值，原 1.0 过严会误判重刷）；seriousCheckResult 直查优先、翻页兜底 | spark 更新版 cx.py |

> ⚠️ 试卷（contentType=2）不在测试版范围内（未实现）。
> 其余逻辑与原版完全一致；若测试版出现异常，请回退正式版 `ewt_brush_v2.py`。

---

## ✨ 功能特性

| 功能 | 说明 |
|---|---|
| 🔐 自动登录 | 账号密码 AES 加密 → oauth 登录，无需手动抓 token |
| 🔍 自动扫描 | 识别全部未完成课时（含时长），只刷必学科目 |
| ⚡ 竞态爆发加速 | 单课时内 N 路并发上报，等效 ~5x 加速（--burst 可调） |
| 🚦 WAF 风控兜底 | 拦截自动冷却 120s 重试（最多 2 次） |
| 🔄 token 自动续期 | 被挤下线自动重新登录（最多 3 次），任务不中断 |
| 🎯 看课检测三重机制 | 弹题绕过 / 检测置过 / 未通过自动重刷（最多 3 次） |
| 🧩 多实例并行 | 自动分片（offset/limit）+ 错峰（phase-offset），速度成倍提升 |
| 📊 实时进度 | 命令行进度条 / 日志文件，实时可见 |
| ✅ 自动验证 | 刷完自动重扫确认，失败课时自动补刷（最多 3 轮） |
| 🆕 原始版本保留 | 收录原作者 spark.py 未修改版本（详见下方） |

---

## 📁 文件结构

```
ewt360-brush/
├── ewt_brush_v2.py          # 刷课引擎（核心，自包含，单文件）—— 增强版
├── ewt_brush_v2_test.py     # 🧪 测试版（登录签名头 + FM/板报直写 + clog补发，不覆盖原版）
├── ewt_brush_easy.py        # 傻瓜式交互入口（提问式引导）
├── requirements.txt         # 依赖（仅 2 个）
├── LICENSE                  # MIT License
├── original/                # 原作者原始脚本（存档与溯源）
│   ├── spark.py             #   原作者 spark 脚本（v1，未修改，1865行）
│   └── README.md            #   原始版说明与对比表
└── docs/
    └── EWT刷课使用教程（傻瓜版）.md  # 详细图文教程
```

> 设计理念：easy 只是遥控器（引导/调度/监控），刷课逻辑全在 v2 引擎里，代码不重复、易维护。

---

## 📦 版本说明（增强版 vs 原始版）

本仓库包含两个版本：

| 能力 | 原始版 spark.py（original/） | 增强版 ewt_brush_v2.py |
|---|---|---|
| 登录方式 | 仅接受已有 token | 账号密码自动登录（AES+oauth）|
| 扫描 | 手动发现作业/课时 | 自动扫描全部未完成课时 |
| 并行 | 单课竞态爆发（~5x）| 外层 N 路并行 + 内层竞态爆发 |
| 多实例 | 不支持 | 支持分片（offset/limit/phase-offset）|
| token 续期 | 无 | 自动续期（最多 3 次）|
| WAF 风控 | 基础限速 | 自动冷却 120s 重试（最多 2 次）|
| 看课检测 | 无 | 三重兜底机制（弹题/置过/重刷）|
| 自动验证 | 无 | 刷完自动重扫 + 失败补刷 |
| 使用复杂度 | 需手动提供 token | 傻瓜式全自动 |

推荐使用增强版（ewt_brush_v2.py），原始版仅作学习参考与协议研究。

---

## 🛠 环境要求

- Python **3.10+**
- 仅需两个依赖：

```bash
pip install -r requirements.txt
# 等价于：
pip install httpx pycryptodome
```

> httpx：网络请求；pycryptodome：AES 加密登录（提供 Crypto 模块）。缺一不可。

---

## 🚀 快速开始

### 方式一：傻瓜式交互（推荐）

```bash
python3 ewt_brush_easy.py
```

按提示依次输入：**账号 → 密码 → 自动扫描 → 实例数/concurrency/burst/qps → Y 确认**
之后全自动：启动 → 监控 → 验证 → 补刷 → 全部刷完。

### 方式二：命令行直接跑（增强版）

```bash
# 预检扫描（先看有哪些课时）
python3 ewt_brush_v2.py --dry-run --account 你的账号 --password 你的密码

# 单实例极速刷
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000

# 4 实例并行（自动分片 + 错峰）
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 0   --limit 25
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 25  --limit 25 --phase-offset 5000
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 50  --limit 25 --phase-offset 10000
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 75  --limit 0  --phase-offset 15000
```

### 方式三：原始版 spark.py（仅接受 token）

```bash
# 提供已有 token 刷全部
python3 original/spark.py --token YOUR_TOKEN --all

# 只刷指定作业
python3 original/spark.py --token YOUR_TOKEN --homework-id 10519926
```

---

## 📱 手机端（Termux）

```bash
pkg install -y python
pip install httpx pycryptodome
termux-setup-storage           # 授权存储
cd /storage/emulated/0/你的脚本目录
termux-wake-lock               # 防息屏断网
python3 ewt_brush_easy.py
```

---

## ⚙️ 参数详解

| 参数 | 默认 | 作用 | 建议 |
|---|---|---|---|
| --concurrency | 12 | 外层路数：一个实例同时刷几个课时 | 6~12，14 可试，16 连接失败 |
| --burst | 12 | 内层路数：单课时内一次发多少条上报 | 12/24/36，越高单轮推进越多 |
| --qps | 400 | 全局限速（请求/分钟） | 100000 = 不限速（推荐） |
| --offset/--limit | 0/0 | 分片：起始下标 + 数量（多实例用） | 自动计算 |
| --phase-offset | 0 | 首轮爆发错峰毫秒（多实例防撞车） | 5000 递增 |
| --hw | 全部 | 只刷指定作业 ID | — |
| --dry-run | 关 | 仅扫描不刷课 | 刷前预检 |
| --speed | 竞态爆发 | 官方倍速模式 0.5~2.0 | 2.0 是硬上限 |
| --force-rounds | 0 | 强制至少跑 N 轮（修复检测） | 3 |
| --force-all | 关 | 强制重刷全部（含已完成） | 慎用 |

> 坑：--qps 0 并不会不限速（代码 if qps and qps > 0 导致退化为默认 120/分钟），真正不限速需传大值（如 100000）。傻瓜入口已自动修正。

---

## 🧠 工作原理

```
登录/读token → 扫描作业课时 → 分批并行（--concurrency 路）
  └─ 每课时：播放上报循环（竞态爆发）→ 查进度 → 停滞则触发兜底机制
       ├─ 机制A：弹题自动答题绕过
       ├─ 机制B：看课检测 addVideoss 两步时序置过
       └─ 机制C：检测未通过自动重刷（最多 3 次）
→ WAF 拦截冷却重试 → token 失效自动续期 → 全部完成 → 汇总验证
```

竞态爆发原理：服务端限流是检查+扣减非原子操作，N 个请求在 30ms 窗口同时到达，多数滑过限流 → 等效 5x 加速。

多实例分片原理：
- 所有实例共用同一 token（避免并发登录互踢）
- 每个实例只处理自己分片内的课时（--offset 起始 + --limit 数量）
- 用 --phase-offset 错峰首轮爆发（避免撞车）
- 实例间互不干扰，速度线性叠加

---

## 📊 实测效率

| 配置 | RATIO | 相当于 |
|---|---|---|
| 单实例 12路/qps400 | ≈ 0.029 | 刷1小时视频需 1.7 分钟 |
| **4实例 × 12路 + 不限速** | **≈ 0.0089** | **刷1小时视频仅需 32 秒** ⭐ |
| 单实例 14路 + burst36 + 不限速 | ≈ 0.033 | 刷1小时视频需 2 分钟 |

结论：多实例并行收益最大（实测 329 课时/40 小时视频，4 实例 21 分钟刷完）。

---

## 🔍 详细教程

详见 [docs/EWT刷课使用教程（傻瓜版）.md](./docs/EWT刷课使用教程（傻瓜版）.md)，包含：
- 完整操作流程
- 多实例分片配置示例
- 概念详解（作业/课时/竞态爆发/WAF/FM 课时）
- 参数调优指南
- 常见问题 FAQ
- 强制重刷方法

---

## ❓ 常见问题

| 现象 | 处理 |
|---|---|
| 获取学校信息失败（空消息）| 网络波动，重跑即可 |
| Token 已失效（2001106）| 自动续期；多实例共用同一 token 文件可避免互踢 |
| EWT 网关 429「安全威胁」| 自动冷却 120s 重试；频繁则降并发 |
| 连接被拒绝 | 并发过高，降到 12 路或更低 |
| 没有未完成的课时 | 全部刷完 ✅ |
| 506 当前账号无权限播放 | 该课时所属作业非本账号权限范围（如未购买的组合课程），无法刷 |
| 想重刷已完成课时 | --force-rounds 3 |
| 账号被封/锁定 | 换账号或联系平台客服 |

---

## 🔒 隐私说明

- 账号密码不会写入任何文件（仅内存中使用）
- token 可选保存至 ~/.ewt_token.txt（可用环境变量 EWT_TOKEN_FILE 指定其他位置）
- 脚本内不含任何账号、密码、token 硬编码
- 网络请求直连 EWT 官方服务器，不经过任何自建中转

---

## ⚠️ 免责声明

- 本工具仅用于学习与研究目的，请仅用于你自己的账号
- 请遵守 EWT360 平台服务条款与所在学校/机构规定，刷课行为可能违反平台规则，后果自负
- 作者不对使用本工具造成的任何账号风险、封禁或其他后果负责
- 请勿将账号密码/token 泄露给他人

---

## 📄 License

[MIT License](./LICENSE) © 2026

---

## 📎 相关链接

| 资源 | 链接 |
|---|---|
| 原作者仓库 | https://github.com/Ruyi0623/spark_ewt |
| 详细教程 | [docs/EWT刷课使用教程（傻瓜版）.md](./docs/EWT刷课使用教程（傻瓜版）.md) |
| 原始版本说明 | [original/README.md](./original/README.md) |

---

## ⭐ 支持

如果本项目对你有帮助，欢迎 Star ⭐ 或提交 Issue / PR。
也欢迎分享你的实测数据（课时数/耗时/配置），帮助优化推荐参数！
