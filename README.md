# 🚀 EWT360 自动刷课工具

> 自包含、多实例并行的 EWT360（升学 e网通）网课平台刷课脚本。
> 自动完成登录、扫描、刷课、监控、验证全流程，单文件即可运行。
> 
> **V3 正式版**（推荐）— 基于 V2 增强，新增 FM/板报直写、clog 补发、过课检测优化、登录签名头降风控、进度条面板、判定阈值优化等。
> **V2 稳定版** — 保留原版，稳定可靠。

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

**实测**：单实例 18路 + burst48 + 不限速下，刷 1 小时视频仅需 **7.6 秒**（效率 RATIO ≈ 0.0021）；实测 1578 分钟视频 **3 分 21 秒刷完**。

---

## 📦 版本说明

| 版本 | 文件 | 说明 |
|---|---|---|
| **V3 正式版** ⭐ | `ewt_brush_v3.py` + `ewt_brush_easy_v3.py` | **推荐使用**。基于 V2 增强，新增 FM/板报 `updateMission` 直写 100%、clog 播放日志补发、过课检测参数优化、登录签名头降风控、**课时级进度条面板**、判定阈值 0.8 优化、进度延迟复核防重刷。 |
| **V2 稳定版** | `ewt_brush_v2.py` + `ewt_brush_easy.py` | 原版保留，稳定可靠，只刷视频课时。 |

---

## ✨ 功能特性（V3）

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
| 🆕 FM/板报直写 | V3 新增：FM 收听 / 板报课时 `updateMission` 一次直写 100% |
| 📡 clog 补发 | V3 新增：完成判定后补发播放日志，提高完成率 |
| 🆕 原始版本保留 | 收录原作者 spark.py 未修改版本（详见下方） |

---

## 📁 文件结构

```
ewt360-brush/
├── ewt_brush_v3.py          # ⭐ V3 引擎（推荐，功能最全）
├── ewt_brush_easy_v3.py     # ⭐ V3 傻瓜式入口（提问式引导）
├── v2/                      # V2 稳定版（单独文件夹）
│   ├── ewt_brush_v2.py      #   V2 引擎
│   └── ewt_brush_easy.py    #   V2 傻瓜式入口
├── requirements.txt         # 依赖（仅 2 个）
├── LICENSE                  # MIT License
├── original/                # 原作者原始脚本（存档与溯源）
│   ├── spark.py             #   原作者 spark 脚本（v1，未修改，1865行）
│   └── README.md            #   原始版说明与对比表
└── docs/
    └── EWT刷课使用教程（傻瓜版）.md  # 详细图文教程
```

> 设计理念：easy 只是遥控器（引导/调度/监控），刷课逻辑全在引擎里，代码不重复、易维护。
> V3 与 V2 分开放置，互不干扰。

---

## 📦 版本对比（V3 vs V2）

| 能力 | V2 稳定版 | V3 正式版 ⭐ |
|---|---|---|
| 视频刷课 | ✅ 竞态爆发加速 | ✅ 同 V2 + 优化 |
| FM/板报直写 | ❌ 不处理 | ✅ `updateMission` 直写 100% |
| clog 播放日志补发 | ❌ 无 | ✅ 补发提高完成判定率 |
| 登录签名头 | ❌ 无 | ✅ 补齐 Web 端签名，降风控 |
| 过课检测参数 | 原始 | ✅ 校本 +2000000、type 按 contentType 计算 |
| 判定阈值 | 1.0（过严） | ✅ 0.8（平台真实阈值） |
| 进度延迟复核 | 立即重刷 | ✅ 等待+复核后再判定 |
| 进度条面板 | 单行计数 | ✅ 课时级进度条面板 |
| 配置限制 | 有上限 | ✅ 无上限，自行决定 |
| 使用复杂度 | 需手动分片 | easy 自动多实例 + 监控 |

---

## 🛠 环境要求

- Python **3.10+**
- **依赖清单（requirements.txt）**：仓库自带 `requirements.txt`，已列出全部依赖（当前仅 2 个），一键安装：

```bash
pip install -r requirements.txt
# 等价于：
pip install httpx pycryptodome
```

> `requirements.txt` 是标准 Python 依赖清单文件，`pip install -r requirements.txt` 会按文件内容自动安装所有依赖并校验版本。
> 当前内容：`httpx>=0.24.0`（网络请求）、`pycryptodome>=3.19.0`（AES 加密登录）。

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

## 🚀 快速开始

### 方式一：傻瓜式交互（推荐，V3）

```bash
python3 ewt_brush_easy_v3.py
```

按提示依次输入：**账号 → 密码 → 自动扫描 → 实例数/concurrency/burst/qps → Y 确认**
之后全自动：启动 → 监控 → 验证 → 补刷 → 全部刷完。
> 💡 V3 傻瓜入口会自动多实例分片 + 显示**课时级进度条面板**，且配置无上限。

### 方式二：命令行直接跑（V3 引擎）

```bash
# 预检扫描（先看有哪些课时）
python3 ewt_brush_v3.py --dry-run --account 你的账号 --password 你的密码

# 🏆 新手推荐：单实例高路数极速刷（实测 3 分 21 秒刷完 400 分钟视频）
python3 ewt_brush_v3.py --account 你的账号 --password 你的密码 \
    --concurrency 18 --burst 48 --qps 100000

# 若想多实例并行（进阶，需手动分片）
python3 ewt_brush_v3.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 0   --limit 25
python3 ewt_brush_v3.py --account 你的账号 --password 你的密码 \
    --concurrency 12 --burst 24 --qps 100000 --offset 25  --limit 25 --phase-offset 5000
```

### 方式三：V2 稳定版 / 原始版 spark.py

**V2 稳定版**（若想用原版）：V2 文件在独立的 `v2/` 文件夹，先进入再运行：
```bash
cd v2
python3 ewt_brush_easy.py       # V2 傻瓜入口
# 或引擎：
python3 ewt_brush_v2.py --account 你的账号 --password 你的密码 --concurrency 18 --burst 48 --qps 100000
```

**原始版 spark.py**（仅接受 token）：
```bash
# 提供已有 token 刷全部
python3 original/spark.py --token YOUR_TOKEN --all
```

---

## 📱 手机端（Termux）

```bash
pkg install -y python
pip install httpx pycryptodome
termux-setup-storage           # 授权存储
cd /storage/emulated/0/你的脚本目录
termux-wake-lock               # 防息屏断网
python3 ewt_brush_easy_v3.py   # V3 傻瓜入口（推荐）
```

---

## ⚙️ 参数详解

| 参数 | 默认 | 作用 | 建议 |
|---|---|---|---|
| --concurrency | 12 | 外层路数：同时刷几个课时 | **新手用 18**（实测 18 路 3 分 21 秒刷完 400 分钟视频），不限速 |
| --burst | 12 | 内层路数：单课时内一次发多少条上报 | **新手用 48**（实测 48 高路数稳定），越高单轮推进越多 |
| --qps | 400 | 全局限速（请求/分钟） | **100000 = 不限速（推荐）** |
| --offset/--limit | 0/0 | 分片：起始下标 + 数量（多实例用） | 自动计算 |
| --phase-offset | 0 | 首轮爆发错峰毫秒（多实例防撞车） | 5000 递增 |
| --hw | 全部 | 只刷指定作业 ID | — |
| --dry-run | 关 | 仅扫描不刷课 | 刷前预检 |
| --speed | 竞态爆发 | 官方倍速模式 0.5~2.0 | 2.0 是硬上限 |
| --force-rounds | 0 | 强制至少跑 N 轮（修复检测） | 3 |
| --force-all | 关 | 强制重刷全部（含已完成） | 慎用 |

> **🏆 新手推荐参数（直接抄）**：
> ```bash
> python3 ewt_brush_v3.py --concurrency 18 --burst 48 --qps 100000
> ```
> **单实例 18路 + burst48 + 不限速**，实测 1578 分钟视频 3 分 21 秒刷完，稳定、无需开多实例。

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
| 4实例 × 12路 + 不限速 | ≈ 0.0089 | 刷1小时视频 32 秒 |
| **单实例 18路 + burst48 + 不限速** | **≈ 0.0021** | **刷 1 小时视频仅 7.6 秒** ⭐⭐ |

**💥 最新实测**：`单实例 18路 | QPS 100000 | burst 48` → **1578 分钟视频（约407小时）仅 3 分 21 秒刷完**，123 课时全部一次通过，无重刷、无卡顿。
```bash
python3 ewt_brush_v3.py --concurrency 18 --burst 48 --qps 100000
```

> **🏆 新手推荐配置：**
> **单实例 18路 + burst48 + 不限速**（最稳，3分21秒刷完400分钟视频）。
> - 多实例并行虽理论更快，但**不稳定、不好操控**（多进程、登录风控、手动分片），**不建议新手追求**。
> - 新手直接跑单实例高路数即可，足够快、够稳、无需开多个终端。

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
| Windows UnicodeEncodeError | 脚本已内置 UTF-8 加固；若仍报错：PowerShell 运行 `$env:PYTHONUTF8="1" ; py ewt_brush_v3.py`；CMD 运行 `set PYTHONUTF8=1 && py ewt_brush_v3.py` |

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

## 💬 联系方式

如果有问题或建议，欢迎通过以下方式联系：

| 方式 | 联系方式 |
|---|---|
| 📮 GitHub Issues | [提交反馈](https://github.com/Zxxaq1478359473/ewt360-brush/issues) |
| 📧 Gmail | zoan0404@gmail.com |
| 💬 QQ | 1478359473 |

---


## 💰 赞赏

如果这个项目帮到了你，省下了大量刷课时间，欢迎扫码请我喝杯咖啡！☕

你的每一份支持，都是我持续维护和更新的最大动力！每一次点赞、每一个 Star、每一杯咖啡，都让我更有热情去完善它 🙏

**🎁 赞赏后你可以：**
- 在 Issues 里留下你的微信昵称，我会拉你进**互助交流群**
- 优先获得新功能和版本体验资格
- 遇到问题时获得**优先响应**

**🏆 累计赞赏记录：**

| 昵称 | 金额 | 时间 |
|---|---|---|
| 北冰洋 | ¥4.00 | 2026-08-21 21:24 |

> 💰 感谢每一位支持者！累计赞赏金额将用于持续维护与开发。

👉 扫一扫，立刻赞赏！扫码备注 `ewt360` 即可，感谢每一份支持 ❤️

<img src="./97640.png" width="600" alt="微信赞赏码">

<a href="https://github.com/Zxxaq1478359473/ewt360-brush/stargazers">
  <img src="https://img.shields.io/github/stars/Zxxaq1478359473/ewt360-brush?style=social&label=Star%20%E6%94%AF%E6%8C%81" alt="Star 支持">
</a>
&nbsp;&nbsp;
<a href="https://github.com/Zxxaq1478359473/ewt360-brush/issues">
  <img src="https://img.shields.io/github/issues/Zxxaq1478359473/ewt360-brush?style=social&label=Issue%20%E5%8F%8D%E9%A6%88" alt="Issue 反馈">
</a>


## ⭐ 支持

如果本项目对你有帮助，欢迎 Star ⭐ 或提交 Issue / PR。
也欢迎分享你的实测数据（课时数/耗时/配置），帮助优化推荐参数！

[![Star History Chart](https://api.star-history.com/svg?repos=Zxxaq1478359473/ewt360-brush&type=Date)](https://star-history.com/#Zxxaq1478359473/ewt360-brush&Date)

