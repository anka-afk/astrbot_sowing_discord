"""Command handler for /史库 <状态|开关|订阅|屏蔽|分级|读空气>."""

from __future__ import annotations

from typing import Any, Optional, Sequence

try:
    from ..ambient.air_reader import AirReader, parse_quiet_hours
    from ..migration import MigrationReport
    from ..models.meme import VALID_NSFW_LEVELS
    from ..sources.base import BaseSource
    from ..sources.hub_source import HubSource
    from ..storage.db import SowingDB
except ImportError:
    from ambient.air_reader import AirReader, parse_quiet_hours
    from migration import MigrationReport
    from models.meme import VALID_NSFW_LEVELS
    from sources.base import BaseSource
    from sources.hub_source import HubSource
    from storage.db import SowingDB


_NSFW_LABELS = {
    0: "0（全年龄安全）",
    1: "1（轻度敏感，默认）",
    2: "2（高能预警）",
}

UNAUTHORIZED_MESSAGE = "仅群主、群管理员或 Bot 管理员可执行此操作。"


class AdminHandler:
    """Per-group management and diagnostics for `/史库`."""

    def __init__(
        self,
        db: SowingDB,
        air_reader: AirReader,
        sources: Sequence[BaseSource],
        *,
        default_group_settings: Optional[dict[str, Any]] = None,
        migration_report: Optional[MigrationReport] = None,
    ):
        self.db = db
        self.air_reader = air_reader
        self.sources = list(sources)
        self.default_group_settings = dict(default_group_settings or {})
        self.migration_report = migration_report

    @staticmethod
    def _requires_authorization(sub: str, rest: list[str]) -> bool:
        """Return True for mutating subcommands and sensitive diagnostics (`订阅`)."""
        sub_l = sub.lower()
        if sub_l in ("状态", "status", "帮助", "help", "-h", "--help"):
            return False
        if sub_l in ("开关", "toggle", "switch"):
            return True
        if sub_l in ("订阅", "sources", "sub"):
            return True
        if sub_l in ("分级", "nsfw"):
            return True
        if sub_l in ("屏蔽", "block", "tags"):
            if not rest or rest[0].lower() in ("列表", "list"):
                return False
            return True
        if sub_l in ("读空气", "air", "ambient"):
            if not rest or rest[0].lower() in ("状态", "status"):
                return False
            return True
        return False

    def handle(
        self,
        group_umo: str,
        args_text: str = "",
        *,
        is_authorized: bool = True,
    ) -> str:
        tokens = [t for t in (args_text or "").strip().split() if t]
        sub = (tokens[0] if tokens else "状态").lower()
        rest = tokens[1:]

        if not is_authorized and self._requires_authorization(sub, rest):
            return UNAUTHORIZED_MESSAGE

        if sub in ("状态", "status"):
            return self._cmd_status(group_umo)
        if sub in ("开关", "toggle", "switch"):
            return self._cmd_toggle(group_umo, rest)
        if sub in ("订阅", "sources", "sub"):
            return self._cmd_subscriptions()
        if sub in ("屏蔽", "block", "tags"):
            return self._cmd_block_tags(group_umo, rest)
        if sub in ("分级", "nsfw"):
            return self._cmd_nsfw(group_umo, rest)
        if sub in ("读空气", "air", "ambient"):
            return self._cmd_air(group_umo, rest)

        return (
            "用法：`/史库 <状态|开关|订阅|屏蔽|分级|读空气>`\n"
            "- `/史库 状态`：查看史库缓存、源同步与本群配置\n"
            "- `/史库 开关 [开|关]`：开启或关闭本群搬史\n"
            "- `/史库 订阅`：查看已接入的 Hub / RSS / 本地源群\n"
            "- `/史库 屏蔽 <添加|移除|清空|列表> [标签]`：管理本群屏蔽标签\n"
            "- `/史库 分级 <0|1|2>`：设置本群 NSFW 上限\n"
            "- `/史库 读空气 [状态|开|关|冷却 <分钟>|免打扰 <HH:MM-HH:MM>]`：管理读空气状态机"
        )

    def _cmd_status(self, group_umo: str) -> str:
        stats = self.db.get_stats()
        cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
        unsent = self.db.count_unsent(group_umo, max_nsfw=int(cfg["max_nsfw"]))
        air = self.air_reader.evaluate(
            group_umo,
            quiet_hours=cfg["quiet_hours"],
            cooldown_sec=int(cfg["cooldown_sec"]),
        )
        lines = [
            "=== 搬史 v2 史库状态 ===",
            f"本地缓存：{stats['cached_items']} 条（本群未发可用：{unsent} 条）",
            f"累计投递：{stats['sent_records']} 条 | 待上报反馈：{stats['pending_feedback']} 条",
            f"本群开关：{'开启' if cfg['enabled'] else '关闭'} | 读空气自动投放：{'开启' if cfg['ambient_enabled'] else '关闭'}",
            f"本群分级上限：{_NSFW_LABELS.get(int(cfg['max_nsfw']), str(cfg['max_nsfw']))}",
            f"本群屏蔽标签：{', '.join(cfg['blocked_tags']) if cfg['blocked_tags'] else '无'}",
            f"当前读空气状态：{air.state.value}（{air.reason}）",
        ]
        if self.migration_report and self.migration_report.migrated:
            lines.append(f"【迁移提示】{self.migration_report.summary}")
        return "\n".join(lines)

    def _cmd_toggle(self, group_umo: str, rest: list[str]) -> str:
        cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
        if not rest:
            new_val = not cfg["enabled"]
        else:
            arg = rest[0].lower()
            if arg in ("开", "on", "true", "1", "启用"):
                new_val = True
            elif arg in ("关", "off", "false", "0", "禁用"):
                new_val = False
            else:
                return "参数无效，请使用：`/史库 开关 开` 或 `/史库 开关 关`"
        self.db.upsert_group_settings(
            group_umo, defaults=self.default_group_settings, enabled=new_val
        )
        return f"已{'开启' if new_val else '关闭'}本群搬史功能。"

    def _cmd_subscriptions(self) -> str:
        if not self.sources:
            return "当前未配置任何内容源，请在 AstrBot WebUI 中启用官方 Hub 或添加 RSS/本地源群。"
        lines = ["=== 已订阅内容源 ==="]
        for src in self.sources:
            cursor = self.db.get_source_cursor(src.source_id) or {}
            status = "启用" if src.enabled else "停用"
            extra = ""
            if isinstance(src, HubSource):
                extra = f" | hub_id={src.hub_id} | seq={cursor.get('next_seq', 0)} | fail={cursor.get('fail_count', 0)}"
            lines.append(f"- [{src.source_type}] {src.source_id} ({status}){extra}")
        return "\n".join(lines)

    def _cmd_block_tags(self, group_umo: str, rest: list[str]) -> str:
        cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
        blocked: list[str] = list(cfg.get("blocked_tags") or [])
        if not rest or rest[0].lower() in ("列表", "list"):
            return f"本群当前屏蔽标签：{', '.join(blocked) if blocked else '无'}"

        op = rest[0].lower()
        if op in ("清空", "clear"):
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, blocked_tags=[]
            )
            return "已清空本群所有屏蔽标签。"

        if op in ("添加", "add") and len(rest) >= 2:
            added = []
            for t in rest[1:]:
                if t not in blocked:
                    blocked.append(t)
                    added.append(t)
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, blocked_tags=blocked
            )
            return f"已添加屏蔽标签：{', '.join(added) if added else '（均已存在）'}；当前：{', '.join(blocked)}"

        if op in ("移除", "删除", "remove", "del") and len(rest) >= 2:
            removed = []
            for t in rest[1:]:
                if t in blocked:
                    blocked.remove(t)
                    removed.append(t)
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, blocked_tags=blocked
            )
            return f"已移除屏蔽标签：{', '.join(removed) if removed else '（无匹配）'}；当前：{', '.join(blocked) if blocked else '无'}"

        return "用法：`/史库 屏蔽 <添加|移除|清空|列表> [标签...]`"

    def _cmd_nsfw(self, group_umo: str, rest: list[str]) -> str:
        cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
        if not rest:
            cur = int(cfg["max_nsfw"])
            return f"本群当前 NSFW 分级上限：{_NSFW_LABELS.get(cur, str(cur))}\n用法：`/史库 分级 <0|1|2>`"
        try:
            level = int(rest[0])
        except ValueError:
            return "分级必须为整数 0、1 或 2。"
        if level not in VALID_NSFW_LEVELS:
            return "分级超出范围，仅支持 0（全年龄安全）、1（轻度敏感）、2（高能预警）。"
        self.db.upsert_group_settings(
            group_umo, defaults=self.default_group_settings, max_nsfw=level
        )
        return f"本群 NSFW 分级上限已更新为：{_NSFW_LABELS[level]}"

    def _cmd_air(self, group_umo: str, rest: list[str]) -> str:
        cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
        if not rest or rest[0].lower() in ("状态", "status"):
            air = self.air_reader.evaluate(
                group_umo,
                quiet_hours=cfg["quiet_hours"],
                cooldown_sec=int(cfg["cooldown_sec"]),
            )
            return (
                f"=== 本群读空气状态 ===\n"
                f"自动投放：{'开启' if cfg['ambient_enabled'] else '关闭'}\n"
                f"状态机：{air.state.value}（可投放={'是' if air.can_send else '否'}）\n"
                f"判定依据：{air.reason}\n"
                f"近 5 分钟消息：{air.msg_count_5m} | 今日人类消息：{air.human_msgs_today}\n"
                f"免打扰：{cfg['quiet_hours']} | 基础冷却：{int(cfg['cooldown_sec']) // 60} 分钟（当前有效：{air.effective_cooldown_sec // 60} 分钟）"
            )

        op = rest[0].lower()
        if op in ("开", "on", "启用"):
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, ambient_enabled=True
            )
            return "已开启本群「读空气」自动破冰投放。"
        if op in ("关", "off", "禁用"):
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, ambient_enabled=False
            )
            return "已关闭本群「读空气」自动投放（仍可手动 `/来坨史`）。"
        if op in ("冷却", "cooldown") and len(rest) >= 2:
            try:
                minutes = max(5, int(rest[1]))
            except ValueError:
                return "冷却分钟数需为正整数，例如：`/史库 读空气 冷却 90`"
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, cooldown_sec=minutes * 60
            )
            return f"本群搬史冷却时间已设为 {minutes} 分钟。"
        if op in ("免打扰", "quiet") and len(rest) >= 2:
            spec = rest[1].strip()
            if parse_quiet_hours(spec) is None:
                return "免打扰格式无效，请使用 HH:MM-HH:MM，例如 `01:00-08:00`。"
            self.db.upsert_group_settings(
                group_umo, defaults=self.default_group_settings, quiet_hours=spec
            )
            return f"本群免打扰时段已更新为 {spec}。"

        return "用法：`/史库 读空气 [状态|开|关|冷却 <分钟>|免打扰 <HH:MM-HH:MM>]`"
