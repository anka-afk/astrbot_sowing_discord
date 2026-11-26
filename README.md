# 🤖 AstrBot Sowing_discord 模块 — 搬史插件 v2.0

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Version](https://img.shields.io/badge/Version-2.0.0-purple)
![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)
[![Contributors](https://img.shields.io/github/contributors/anka-afk/astrbot_sowing_discord?color=green)](https://github.com/anka-afk/astrbot_sowing_discord/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/anka-afk/astrbot_sowing_discord)](https://github.com/anka-afk/astrbot_sowing_discord/commits/main)

</div>

<div align="center">

[![Moe Counter](https://count.getloli.com/get/@GalChat?theme=moebooru)](https://github.com/anka-afk/astrbot_sowing_discord)

</div>

> **让 AI 学会搬史，搬史进入 2.0 新时代！全自动、零加群、全平台、读空气搬史！**

---

## 📢 公告与社区入口

> **⚠️ 公益免费声明**
> 本项目为**纯公益项目**，不接受任何赞助，所有服务均为免费提供。**所有付费服务与广告都是骗人的**，请不要相信并向作者报告。
> 注意：不要相信任何你无法准确证明真实的消息，这可能是谣言。

### 🔗 官方在线入口

| 入口 | 地址 | 说明 |
| :--- | :--- | :--- |
| **🌐 精选史库广场** | https://banshi.ankaanka.org | 在线浏览社区已过审的高清史库（图文、GIF、聊天记录） |
| **📮 在线真人投稿** | https://banshi.ankaanka.org/submit | 直接在网页投稿图文 / GIF / 结构化聊天记录（支持自动去重与脱敏提醒） |
| **🧭 发布页 & 加群入口** | https://sowingdiscord.ankaanka.org | **收藏发布页不迷路~** 获取最新群聊入口与社区公告 |
| **🏗️ 中心站开源仓库** | https://github.com/anka-afk/sowing-discord-hub | Hub 后端、Web 审核台与单号投稿群采集器开源代码，支持自建联邦史站 |
| **📦 v1 旧版归档** | [`v1-legacy` 分支](https://github.com/anka-afk/astrbot_sowing_discord/tree/v1-legacy) / [`v1.0-archive`](https://github.com/anka-afk/astrbot_sowing_discord/releases/tag/v1.0-archive) | 保留旧版源码归档（不再维护，强烈建议升级 v2） |

### 💬 加入搬史群聊 / 为项目做贡献

- 请前往发布页加群：**https://sowingdiscord.ankaanka.org**（收藏发布页不迷路~）
- **重要说明**：所有群（包括聊天群，**请在聊天群聊天**）请从**入口群**加入，因为所有群基本都无法直接搜索到了。入口群的群简介中有其他群链接和群号；加入其他群后可以退出入口群（入口群永久禁言）。
- 群聊入群问题的答案都是**保证可以搜索到的**，你并不需要懂得答案的由来，只需要证明具有基础的搜索与解决问题的能力。
- **v2 好消息**：**你的 Bot 现在已经完全不需要加任何公共 QQ 源群了！** 安装插件后默认直连官方公益史库，大幅降低账号风控与连坐风险；群友如果想贡献好史，既可以加群共建，也可以直接在网页 https://banshi.ankaanka.org/submit 或在自己群内发送 `/投稿`！

---

## ✨ v2.0 重大升级亮点

1. **零加群开箱即用**：彻底告别 v1「所有 Bot 挤进 QQ 源群 + 暴力轮询贴表情」的高风控架构。v2 默认直连官方公益 Hub（`https://banshi-api.ankaanka.org`），后台自动完成轻量 16-bit PoW 匿名注册，开箱即搬。
2. **真正跨平台 & 原生嵌套聊天记录**：
   - 基于 AstrBot 通用 `MessageChain` 分发，无缝支持 **QQ / Telegram / Discord / 微信 / 企微 / 飞书**。
   - **QQ 端默认开启原生合并转发**（`qq_native_forward: true`），支持最多 **3 层套娃嵌套聊天记录 / 50 条对话 / 20 张原图** 还原成 QQ 原生转发卡片（自动脱敏为 `群友A/B`，发送失败自动无缝回退发高清切片图）。
   - **非 QQ 平台**自动发送 `1080×≤1920` 高清 WebP 切片图，零 Playwright/Chromium 依赖。
3. **五态「读空气」智能破冰**：纯内存滑动窗口（不存群聊原文）自动判断群状态：
   - 免打扰时段（默认 `01:00-08:00`）禁发
   - 热聊中（5 分钟人类发言 ≥ 6 条）**绝不打断**
   - 死群（超 24 小时无人类发言）**绝不空投刷屏**
   - 刚冷场窗口（15~45 分钟）自动破冰；若上条史无人接话，冷却时间自动翻倍退避
4. **多源联邦 & 私有群兼容**：支持同时挂载官方 Hub、多个社区自建 Hub、通用 RSS/Atom/RSSHub 图文源，以及**事件驱动的本地自用源群**（被动监听 `group_msg_emoji_like`，彻底废除轮询）。
5. **社区共建闭环**：
   - `/投稿`：在自己群里回复好笑的消息一键投稿（本地自动脱敏 + 重复早停 + 拦截原始视频）。
   - 匿名反馈闭环 & 下架墓碑同步：`/feed` 自动接收下架 `tombstones` 秒级清理本地缓存；可选开启用户自有 LLM 简短吐槽（5s 硬超时静默降级）。

---

## 🛠️ 安装与老用户升级指南

### 新用户安装
1. 在 AstrBot 插件市场搜索安装，或通过仓库链接安装：`https://github.com/anka-afk/astrbot_sowing_discord`
2. 在 AstrBot WebUI 中填入你想开启搬史的 `target_groups`（目标群号），保存重载即可开搬（无需加任何 QQ 源群！）。

### 老用户（v1 → v2）无损升级
- 直接更新插件并重启 AstrBot，v2 会自动检测旧版 `banshi_*` 配置：
  1. 自动将旧配置完整备份为插件数据目录下的 `config.v1.bak.json`；
  2. 自动把旧 `banshi_target_list` 迁入 `target_groups`，旧 `banshi_group_list` 迁入事件驱动的 `local_onebot_groups`；
  3. 同时默认开启官方公益 Hub（如果只想搬运自己原有的私有源群，在 WebUI 关闭 `official_hub_enabled` 即可）。

---

## 🎮 常用指令

| 指令 | 别名 | 说明 |
| :--- | :--- | :--- |
| `/来坨史 [标签]` | `/搬史` | 手动从史库拉取一条本群未发过的好史（可指定标签，如 `/来坨史 抽象`） |
| `/投稿 [标签/附言]` | — | 引用任意群消息或附带图文投稿至官方审核池（自动脱敏 `群友A/B`、重复早停） |
| `/烂史 [原因]` | — | 引用 Bot 发出的搬史消息点踩或举报（原因：`nsfw_unmarked` / `privacy_leak` / `duplicate` 等） |
| `/史库 <子命令>` | — | 群管热管理：`状态` / `开关 <开\|关>` / `订阅` / `屏蔽 <标签>` / `分级 <0\|1\|2>` / `读空气 <开\|关>` |

---

## ⚙️ 核心配置说明（AstrBot WebUI 可视化配置）

- `official_hub_enabled` / `official_hub_url`：启用官方公益 Hub（默认 `true` / `https://banshi-api.ankaanka.org`）
- `extra_hubs`：额外订阅的社区自建联邦 Hub 列表
- `rss_sources`：通用 RSS / Atom / RSSHub 图文源列表
- `local_onebot_groups` / `local_emoji_threshold`：可选本地事件驱动源群及贴表情阈值（默认 `3`）
- `target_groups`：目标搬史群列表（支持 `unified_msg_origin` 或纯群号）
- `default_max_nsfw`：内容分级上限（`0` 全年龄安全 / `1` 轻度敏感 [默认] / `2` 高能预警）
- `ambient_enabled` / `quiet_hours` / `cooldown_sec`：五态「读空气」智能投放开关、免打扰时段与冷却秒数
- `qq_native_forward`：QQ 端优先原生合并转发聊天记录（默认 `true`，失败自动回退高清切片图）
- `llm_comment_prob` / `llm_timeout_sec`：可选复用 AstrBot LLM 简短吐槽的概率与硬超时（默认 `0.3` / `5.0s`）
- `user_token`：可选，可信投稿人 `X-User-Token`（用于 `/投稿` 高优通道）

---

## ❓ 常见问题

1. **Q: 为什么装好后群里没有立刻自动搬史？**
   - A: 请先确认在配置里把当前群加入了 `target_groups`（或直接在群里发 `/来坨史` 测试是否通畅）。自动搬史启用了「读空气」机制：夜间免打扰时段不发、群友正在热聊时不打断、死群不刷屏，只有在群聊刚冷场 `15~45 分钟` 的窗口才会自动破冰投放。
2. **Q: 如何向官方史库投稿？**
   - A: 两种方式任选：① 打开网页 **https://banshi.ankaanka.org/submit** 直接上传图文/GIF/聊天记录；② 在任意群里**回复**一条好笑的消息并发送 `/投稿 [标签]`，插件会自动脱敏并提交审核，审核通过后全网共享！
3. **Q: 为什么聊天记录里的图片会被自动脱敏？**
   - A: 为保护群友隐私，插件和采集器在上传前会自动抹除真实 QQ 号、群号与头像，按序替换为 `群友A / 群友B`，同时人工审核员会对疑似隐私信息进行二次复核。

---

## 👥 贡献指南

欢迎通过以下方式参与项目：

- 📮 在 https://banshi.ankaanka.org/submit 投稿优质好史
- 🐛 提交 Issue 报告问题或反馈建议
- 🔧 提交 Pull Request 改进插件或中心站代码（[sowing-discord-hub](https://github.com/anka-afk/sowing-discord-hub)）

## 📄 许可证

本项目基于 MIT 许可证开源。
