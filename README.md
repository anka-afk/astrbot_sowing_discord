# 🤖 AstrBot Sowing Discord v2 — 搬史自动化 2.0

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue)
![Version](https://img.shields.io/badge/Version-2.0.0-purple)

> 彻底告别 v1「全员 Bot 加 QQ 源群 + 暴力轮询贴表情 + 风控连坐 + 仅支持 QQ」架构，迈入**零加群、全平台、读空气、社区众包共建**的搬史新时代！

## ✨ v2.0 重大升级

- **零加群开箱即用**：默认直连公益中心站 Hub（[sowing-discord-hub](https://github.com/anka-afk/sowing-discord-hub)），后台自动完成轻量 16-bit SHA-256 PoW 匿名注册，无需让 Bot 加入任何公共 QQ 源群，大幅降低封号与源群连坐风险。
- **真正跨平台 & 零重依赖**：基于 AstrBot 通用 `MessageChain` 发送，支持 **QQ / Telegram / Discord / 微信 / 企微 / 飞书**；聊天记录由审核台浏览器端预渲染为 `720×≤1800` 高清 WebP 切片（QQ 端也可选尝试原生合并转发并自动回退），插件端零 Playwright/Chromium 依赖。
- **多源可插拔架构 (`Source -> Pipeline -> Sink`)**：
  - 官方公益 Hub & 多自定义 Hub 联邦订阅（`after_seq` + `ETag`/`304` + 下架墓碑 `tombstones` 秒级清缓存）
  - 通用 RSS / Atom / RSSHub 图文订阅（纯 Python 标准库解析）
  - 保留本地自用源群模式，彻底废除暴力轮询，改用 OneBot `group_msg_emoji_like` 被动事件驱动
- **五态「读空气」智能破冰**：纯内存滑动窗口区分免打扰、冷却、热聊中（绝不打断）、死群（绝不空投刷屏）、刚冷场破冰（上条无人理自动冷却翻倍退避）。
- **社区共建闭环**：群内回复消息发送 `/投稿 [标签]` 自动本地脱敏（抹除真实 QQ/群号/头像，映射为 `群友A/B`）并上传查重；可选开启群友互动匿名反馈与用户自有 LLM 简短吐槽（5s 硬超时静默降级）。
- **v1 → v2 无损平滑迁移**：升级启动时自动检测旧版 `banshi_*` 配置，先备份 `config.v1.bak.json` 并自动映射目标群与本地源群。

## 🛠️ 常用指令

| 指令 | 说明 |
| :--- | :--- |
| `/来坨史 [标签]`（别名 `/搬史`） | 手动从史库拉取一条未发过的好史发送到当前群 |
| `/投稿 [标签/附言]` | 引用任意群消息或附带图文投稿至 Hub（自动本地脱敏 + 重复早停，拦截原始视频） |
| `/烂史 [原因]` | 引用 Bot 搬运的消息反馈点踩或举报（原因：`nsfw_unmarked` / `privacy_leak` / `duplicate` 等） |
| `/史库 <状态\|开关\|订阅\|屏蔽\|分级\|读空气>` | 群管免改配置热查看状态与调整群级策略 |

## ⚙️ 主要配置项（AstrBot WebUI 可视化配置）

- `official_hub_enabled` / `official_hub_url`：是否启用官方 Hub 及 API 根地址（默认 `https://banshi-api.ankaanka.org`，可随时指向自建 Hub）。
- `extra_hubs` / `rss_sources`：追加多个联邦 Hub 或 RSS/RSSHub 图文源。
- `local_onebot_groups` / `local_emoji_threshold`：可选本地事件驱动源群与贴表情阈值。
- `target_groups` / `default_max_nsfw` / `default_blocked_tags`：目标搬史群、内容分级上限（`0` 安全 / `1` 轻度敏感 / `2` 高能预警）及屏蔽标签。
- `ambient_enabled` / `quiet_hours` / `cooldown_sec`：五态「读空气」投放参数。
- `llm_comment_prob` / `qq_native_forward`：可选用户自有 LLM 吐槽概率、QQ 端优先原生合并转发开关。

## 🔗 相关项目与发布页

- 中心站服务端、Web 广场与极速审核台、单号采集器：[anka-afk/sowing-discord-hub](https://github.com/anka-afk/sowing-discord-hub)
- 官方发布页：https://sowingdiscord.ankaanka.org

## 📄 许可证

MIT License
