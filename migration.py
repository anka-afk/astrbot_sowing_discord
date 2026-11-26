"""Automatic lossless v1 -> v2 configuration detection, backup, and migration."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional

logger = logging.getLogger(__name__)

V1_SIGNATURE_KEYS = (
    "banshi_group_list",
    "banshi_target_list",
    "banshi_cooldown_seconds",
    "banshi_cooldown_minutes",
    "banshi_cooldown_min",
    "banshi_cooldown_sec",
    "banshi_emoji_threshold",
    "banshi_quiet_hours",
    "banshi_interval",
)


@dataclass
class MigrationReport:
    migrated: bool = False
    backup_path: Optional[str] = None
    mapped_target_groups: list[str] = field(default_factory=list)
    mapped_local_sources: list[str] = field(default_factory=list)
    mapped_cooldown_sec: Optional[int] = None
    mapped_emoji_threshold: Optional[int] = None
    official_hub_enabled: bool = True
    summary: str = ""


def detect_v1_config(config: Mapping[str, Any]) -> bool:
    """Return True if the config contains any legacy v1 keys."""
    if not isinstance(config, Mapping):
        return False
    for key in config.keys():
        if key in V1_SIGNATURE_KEYS or str(key).startswith("banshi_"):
            return True
    return False


def _normalize_group_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [item.strip() for item in raw.replace("，", ",").split(",") if item.strip()]
    if isinstance(raw, (list, tuple, set)):
        result: list[str] = []
        for item in raw:
            s = str(item).strip()
            if s and s not in result:
                result.append(s)
        return result
    return []


def _extract_cooldown_sec(config: Mapping[str, Any]) -> Optional[int]:
    if "banshi_cooldown_sec" in config and config["banshi_cooldown_sec"] is not None:
        return max(60, int(config["banshi_cooldown_sec"]))
    if "banshi_cooldown_seconds" in config and config["banshi_cooldown_seconds"] is not None:
        return max(60, int(config["banshi_cooldown_seconds"]))
    if "banshi_cooldown_min" in config and config["banshi_cooldown_min"] is not None:
        return max(60, int(float(config["banshi_cooldown_min"]) * 60))
    if "banshi_cooldown_minutes" in config and config["banshi_cooldown_minutes"] is not None:
        return max(60, int(float(config["banshi_cooldown_minutes"]) * 60))
    # Fallback: scan any banshi_cooldown_* key
    for k, v in config.items():
        ks = str(k)
        if ks.startswith("banshi_cooldown_") and v is not None:
            try:
                num = float(v)
                if "min" in ks:
                    return max(60, int(num * 60))
                if "hour" in ks or "hr" in ks:
                    return max(60, int(num * 3600))
                return max(60, int(num))
            except (TypeError, ValueError):
                continue
    return None


def _is_posix() -> bool:
    return os.name == "posix"


def migrate_v1_to_v2(
    config: MutableMapping[str, Any],
    data_dir: str | Path,
) -> tuple[dict[str, Any], MigrationReport]:
    """
    Detect legacy v1 configuration keys, save a full backup to
    `<data_dir>/config.v1.bak.json`, and losslessly map them to v2 keys.

    Returns `(new_v2_config_dict, MigrationReport)`.
    """
    if not detect_v1_config(config):
        return dict(config), MigrationReport(migrated=False)

    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    backup_file = data_path / "config.v1.bak.json"

    # Full backup of raw v1 config before mutating anything
    raw_snapshot = dict(config)
    backup_payload = json.dumps(raw_snapshot, ensure_ascii=False, indent=2, default=str)
    if _is_posix():
        fd = os.open(str(backup_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(backup_payload)
        try:
            os.chmod(backup_file, 0o600)
        except OSError:
            pass
    else:
        backup_file.write_text(backup_payload, encoding="utf-8")

    migrated_cfg = dict(config)

    # 1. Target groups: banshi_target_list -> target_groups
    legacy_targets = _normalize_group_list(config.get("banshi_target_list"))
    existing_targets = _normalize_group_list(migrated_cfg.get("target_groups"))
    merged_targets = list(dict.fromkeys([*existing_targets, *legacy_targets]))
    migrated_cfg["target_groups"] = merged_targets

    # 2. Source groups: banshi_group_list -> local_onebot_groups (event-driven, no polling)
    legacy_sources = _normalize_group_list(config.get("banshi_group_list"))
    existing_sources = _normalize_group_list(migrated_cfg.get("local_onebot_groups"))
    merged_sources = list(dict.fromkeys([*existing_sources, *legacy_sources]))
    migrated_cfg["local_onebot_groups"] = merged_sources

    # 3. Cooldown mapping
    cooldown_sec = _extract_cooldown_sec(config)
    if cooldown_sec is not None:
        migrated_cfg["cooldown_sec"] = cooldown_sec

    # 4. Quiet hours & emoji threshold
    if "banshi_quiet_hours" in config and config["banshi_quiet_hours"]:
        migrated_cfg["quiet_hours"] = str(config["banshi_quiet_hours"])

    emoji_threshold: Optional[int] = None
    for key in ("banshi_emoji_threshold", "banshi_like_threshold"):
        if key in config and config[key] is not None:
            try:
                emoji_threshold = max(1, int(config[key]))
                migrated_cfg["local_emoji_threshold"] = emoji_threshold
                break
            except (TypeError, ValueError):
                pass

    # 5. Enable official Hub by default (can be toggled off in WebUI to keep only private sources)
    if "official_hub_enabled" not in migrated_cfg:
        migrated_cfg["official_hub_enabled"] = True

    # Remove legacy v1 keys from migrated v2 config
    for k in list(migrated_cfg.keys()):
        if k in V1_SIGNATURE_KEYS or str(k).startswith("banshi_"):
            migrated_cfg.pop(k, None)

    summary = (
        f"已自动完成 v1 -> v2 配置迁移（原配置已备份至 {backup_file.name}）："
        f"映射目标群 {len(legacy_targets)} 个、本地源群 {len(legacy_sources)} 个"
        "（已自动升级为事件驱动监听，停用旧版轮询），并默认启用官方公益 Hub。"
        "如需仅保留私有源群，可在 AstrBot WebUI 中一键关闭官方 Hub。"
    )
    report = MigrationReport(
        migrated=True,
        backup_path=str(backup_file),
        mapped_target_groups=legacy_targets,
        mapped_local_sources=legacy_sources,
        mapped_cooldown_sec=cooldown_sec,
        mapped_emoji_threshold=emoji_threshold,
        official_hub_enabled=bool(migrated_cfg.get("official_hub_enabled", True)),
        summary=summary,
    )
    logger.info(summary)
    return migrated_cfg, report
