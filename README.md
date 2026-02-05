# 🤖 Astrbot Sowing_discord 模块 - 搬史插件

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![Python Version](https://img.shields.io/badge/Python-3.10.14%2B-blue)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)
[![PRs Welcome](https://img.shields.io/badge/PRs-Welcome-brightgreen)](CONTRIBUTING.md)
[![Contributors](https://img.shields.io/github/contributors/anka-afk/astrbot_plugin_meme_manager?color=green)](https://github.com/anka-afk/astrbot_plugin_meme_manager/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/anka-afk/astrbot_plugin_meme_manager)](https://github.com/anka-afk/astrbot_plugin_meme_manager/commits/main)

</div>

<div align="center">

[![Moe Counter](https://count.getloli.com/get/@GalChat?theme=moebooru)](https://github.com/anka-afk/astrbot_sowing_discord)

</div>

让 AI 学会搬史, 搬史进入新时代! 全自动搬史!

## 📢 公告: 注意 当前项目正在重构中

由于意料之外的热度, 本项目已经不适合直接使用(让 bot 转发未经筛选的, 不可信任来源的消息过于危险, 这不仅会导致您的 bot 被封禁, 也可能导致群聊被封禁, 面对传播谣言等风险)(当前版本为 v0.9, 已经附带简单的评价机制, 但仍然需要完善)

本项目正在重构中, 每完成一个目标将会更新:

- [x] 建立评价机制, 只有满足一定评价条件的消息才会被转发(在此之前, 不建议使用, 除非来源可以信任)(v0.9, 当前为粗略分类, 没有进行精细分类, 但已经可以正常使用)
- [x] 史的保存(v0.9), 由于史的来源的不确定性, 需要保存并去重(未实现去重), 对于少量消息本地缓存是不错的策略, 对于大量消息, 就应当使用数据库或服务器中心化保存
- [ ] 史的自动评价, 我们会训练一个模型来自动评价史的类别与等级, 将来会开放给大家免费使用

本项目为公益项目, 不接受任何赞助, 所有服务均为免费提供, 所有付费服务与广告都是骗人的, 请不要相信并向作者报告

如果您想加入群聊, 为项目做出贡献, 请添加:

请前往发布页加群:
发布页:
https://ankaanka.me/sowing_discord_center/
收藏发布页不迷路~
所有群，包括聊天群（请在聊天群聊天）请从入口群加入，因为所有群基本都无法搜索了，入口群群简介有其他群链接和群号，入口群加入其他群后可以退出（或者你可以不加这个群，这个群将会永久禁言)。

- 群聊的答案都是**保证可以搜索的**， 你并不需要懂得答案的由来, 你只是需要证明具有基础的搜索与解决问题的能力 

注意, 不要相信任何你无法准确证明真实的消息, 这可能是谣言

## ❓ 常见问题

1. **Q: 为什么我的 bot 没有自动搬史?**

   - A: 插件逻辑如下:
     - 1. 插件会从指定的群聊(搬史源头)中获取消息, 并缓存消息
     - 2. 10 分钟后, 插件会获取消息的贴表情信息, 如果满足评价条件(好史), 就会触发搬史
     - 3. 因此你至少需要等待 10 分钟, 插件才会开始搬史

## ✨ 功能

- 🚫 自动从指定的群聊(推荐加入 1032915502, 共建搬史中心), 转发聊天记录(史)到其他指定的群聊(详见设置)

## 🛠️ 使用方法

1. 加入天天发史的群聊
2. 设置该群聊为搬史源头, 设置目标群聊
3. bot 开始全自动搬史!

## ⚙️ 配置说明

插件配置项包括：

- `banshi_interval`: 搬史间隔, 单位: 秒（兼容项，实际冷却由时间段动态决定）
- `banshi_cache_seconds`: 消息缓存时间限制, 也是转发前检查历史消息的时间窗口 (秒)
- `banshi_cooldown_day_seconds`: 动态冷却-白天冷却秒数（默认 600）
- `banshi_cooldown_night_seconds`: 动态冷却-夜间冷却秒数（默认 3600）
- `banshi_cooldown_day_start`: 动态冷却-白天时段起始时间（HH:MM，默认 09:00）
- `banshi_cooldown_night_start`: 动态冷却-夜间时段起始时间（HH:MM，默认 01:00）
- `banshi_group_list`: 史的来源群列表
- `banshi_target_list`: 白名单, 史的目标列表, 可以填写多个群或用户, 默认空白即全部群
- `block_source_messages`: 开启后将屏蔽 banshi_group_list 中的群消息

## 👥 贡献指南

欢迎通过以下方式参与项目：

- 🐛 提交 Issue 报告问题
- 💡 提出新功能建议
- 🔧 提交 Pull Request 改进代码

## 📄 许可证

本项目基于 MIT 许可证开源。
