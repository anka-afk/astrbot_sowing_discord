"""Pure in-memory 5-state 'Air Reader' (读空气) ambient delivery state machine."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class AirState(str, Enum):
    QUIET_HOURS = "QUIET_HOURS"
    COOLDOWN = "COOLDOWN"
    ACTIVE_CHAT = "ACTIVE_CHAT"
    DEAD_GROUP = "DEAD_GROUP"
    LULL_READY = "LULL_READY"


@dataclass
class AirCheckResult:
    state: AirState
    can_send: bool
    reason: str
    effective_cooldown_sec: int
    msg_count_5m: int
    human_msgs_today: int
    silence_sec: Optional[float]
    unanswered_streak: int


@dataclass
class GroupAirState:
    """Pure in-memory sliding-window metrics per group (never stores chat text)."""

    last_msg_ts: Optional[float] = None
    last_human_msg_ts: Optional[float] = None
    last_sender_is_bot: bool = False
    last_meme_sent_ts: Optional[float] = None
    unanswered_streak: int = 0
    human_msg_timestamps: deque[float] = field(default_factory=deque)


def parse_quiet_hours(spec: str) -> Optional[tuple[int, int]]:
    """Parse 'HH:MM-HH:MM' into (start_minute_of_day, end_minute_of_day)."""
    if not spec or "-" not in spec:
        return None
    try:
        start_s, end_s = spec.strip().split("-", 1)
        sh, sm = [int(x) for x in start_s.strip().split(":", 1)]
        eh, em = [int(x) for x in end_s.strip().split(":", 1)]
        if not (0 <= sh <= 23 and 0 <= sm <= 59 and 0 <= eh <= 23 and 0 <= em <= 59):
            return None
        return sh * 60 + sm, eh * 60 + em
    except ValueError:
        return None


def is_in_quiet_hours(now_dt: datetime, quiet_hours: str) -> bool:
    parsed = parse_quiet_hours(quiet_hours)
    if parsed is None:
        return False
    start_m, end_m = parsed
    if start_m == end_m:
        return False
    cur_m = now_dt.hour * 60 + now_dt.minute
    if start_m < end_m:
        return start_m <= cur_m < end_m
    # Crosses midnight, e.g. 23:00-08:00
    return cur_m >= start_m or cur_m < end_m


class AirReader:
    """
    Five-state ambient delivery controller:
    1. QUIET_HOURS  : quiet hours window (default 01:00-08:00) -> block
    2. COOLDOWN     : within cooldown (doubled exponentially if previous meme ignored) -> block
    3. ACTIVE_CHAT  : >=6 human messages in last 5 min, or human spoke < lull_min_sec ago -> block
    4. DEAD_GROUP   : >24h with no human speech, no speech today, or stale > lull_max_sec -> block
    5. LULL_READY   : active today + 15~45 min since last human msg + last sender is NOT bot -> send
    """

    def __init__(
        self,
        *,
        default_quiet_hours: str = "01:00-08:00",
        default_cooldown_sec: int = 5400,
        active_chat_threshold_5m: int = 6,
        lull_min_sec: int = 900,
        lull_max_sec: int = 2700,
        dead_group_sec: int = 86400,
        engagement_window_sec: int = 1800,
        max_backoff_exponent: int = 5,
    ):
        self.default_quiet_hours = default_quiet_hours
        self.default_cooldown_sec = int(default_cooldown_sec)
        self.active_chat_threshold_5m = int(active_chat_threshold_5m)
        self.lull_min_sec = int(lull_min_sec)
        self.lull_max_sec = int(lull_max_sec)
        self.dead_group_sec = int(dead_group_sec)
        self.engagement_window_sec = int(engagement_window_sec)
        self.max_backoff_exponent = int(max_backoff_exponent)
        self._groups: dict[str, GroupAirState] = {}

    def get_state(self, group_umo: str) -> GroupAirState:
        st = self._groups.get(group_umo)
        if st is None:
            st = GroupAirState()
            self._groups[group_umo] = st
        return st

    def _prune_timestamps(self, st: GroupAirState, now_ts: float) -> None:
        cutoff = now_ts - max(self.dead_group_sec, 86400)
        while st.human_msg_timestamps and st.human_msg_timestamps[0] < cutoff:
            st.human_msg_timestamps.popleft()

    def record_human_message(self, group_umo: str, now_ts: Optional[float] = None) -> None:
        """Record a human message timestamp without storing any message content."""
        ts = float(now_ts if now_ts is not None else time.time())
        st = self.get_state(group_umo)
        st.last_msg_ts = ts
        st.last_human_msg_ts = ts
        st.human_msg_timestamps.append(ts)
        self._prune_timestamps(st, ts)

        # If a human speaks within engagement_window_sec after our last meme, reset unanswered streak
        if st.last_sender_is_bot and st.last_meme_sent_ts is not None:
            if (ts - st.last_meme_sent_ts) <= self.engagement_window_sec:
                st.unanswered_streak = 0
        st.last_sender_is_bot = False

    def record_bot_message(
        self,
        group_umo: str,
        *,
        is_meme: bool = True,
        now_ts: Optional[float] = None,
    ) -> None:
        """Record a message sent by the bot itself."""
        ts = float(now_ts if now_ts is not None else time.time())
        st = self.get_state(group_umo)
        st.last_msg_ts = ts
        st.last_sender_is_bot = True
        if is_meme:
            st.last_meme_sent_ts = ts
            st.unanswered_streak = min(st.unanswered_streak + 1, self.max_backoff_exponent)

    def effective_cooldown_sec(
        self, group_umo: str, base_cooldown_sec: Optional[int] = None
    ) -> int:
        base = int(base_cooldown_sec if base_cooldown_sec is not None else self.default_cooldown_sec)
        st = self.get_state(group_umo)
        if st.unanswered_streak <= 0:
            return base
        return base * (2 ** min(st.unanswered_streak, self.max_backoff_exponent))

    def _count_metrics(self, st: GroupAirState, now_ts: float, now_dt: datetime) -> tuple[int, int]:
        self._prune_timestamps(st, now_ts)
        five_min_ago = now_ts - 300.0
        day_start_ts = now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        msg_5m = 0
        today = 0
        for ts in st.human_msg_timestamps:
            if ts >= five_min_ago and ts <= now_ts:
                msg_5m += 1
            if ts >= day_start_ts and ts <= now_ts:
                today += 1
        return msg_5m, today

    def evaluate(
        self,
        group_umo: str,
        *,
        now_dt: Optional[datetime] = None,
        quiet_hours: Optional[str] = None,
        cooldown_sec: Optional[int] = None,
    ) -> AirCheckResult:
        """Evaluate the current state of `group_umo` in the 5-state FSM."""
        dt = now_dt if now_dt is not None else datetime.now()
        now_ts = dt.timestamp()
        qh = quiet_hours if quiet_hours is not None else self.default_quiet_hours
        eff_cd = self.effective_cooldown_sec(group_umo, cooldown_sec)

        st = self.get_state(group_umo)
        msg_5m, human_today = self._count_metrics(st, now_ts, dt)
        silence = (now_ts - st.last_human_msg_ts) if st.last_human_msg_ts is not None else None

        # 1. QUIET_HOURS
        if is_in_quiet_hours(dt, qh):
            return AirCheckResult(
                state=AirState.QUIET_HOURS,
                can_send=False,
                reason=f"免打扰时段 ({qh})",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # 2. COOLDOWN (including unanswered-meme exponential backoff & anti-monologue guard)
        if st.last_meme_sent_ts is not None and (now_ts - st.last_meme_sent_ts) < eff_cd:
            remaining = int(eff_cd - (now_ts - st.last_meme_sent_ts))
            return AirCheckResult(
                state=AirState.COOLDOWN,
                can_send=False,
                reason=f"冷却中（剩余 {remaining}s，有效冷却 {eff_cd}s）",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        if st.last_sender_is_bot:
            return AirCheckResult(
                state=AirState.COOLDOWN,
                can_send=False,
                reason="上一条消息由 Bot 发出且尚无人接话，禁止自说自话刷屏",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # 3. ACTIVE_CHAT
        if msg_5m >= self.active_chat_threshold_5m:
            return AirCheckResult(
                state=AirState.ACTIVE_CHAT,
                can_send=False,
                reason=f"近 5 分钟人类消息 {msg_5m} 条，正在热聊不打断",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # 4. DEAD_GROUP
        if silence is None or silence > self.dead_group_sec or human_today <= 0:
            return AirCheckResult(
                state=AirState.DEAD_GROUP,
                can_send=False,
                reason="超 24 小时无人类发言或今日无活跃，禁止向死群空投",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # Still recently chatting (< lull_min_sec) -> treat as ACTIVE_CHAT
        if silence < self.lull_min_sec:
            return AirCheckResult(
                state=AirState.ACTIVE_CHAT,
                can_send=False,
                reason=f"距最后人类发言仅 {int(silence)}s（未满冷场下限 {self.lull_min_sec}s）",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # Past the fresh-lull window (> lull_max_sec) -> group has gone cold
        if silence > self.lull_max_sec:
            return AirCheckResult(
                state=AirState.DEAD_GROUP,
                can_send=False,
                reason=f"距最后人类发言已 {int(silence)}s（超过刚冷场窗口上限 {self.lull_max_sec}s）",
                effective_cooldown_sec=eff_cd,
                msg_count_5m=msg_5m,
                human_msgs_today=human_today,
                silence_sec=silence,
                unanswered_streak=st.unanswered_streak,
            )

        # 5. LULL_READY
        return AirCheckResult(
            state=AirState.LULL_READY,
            can_send=True,
            reason=f"刚进入冷场窗口（沉默 {int(silence)}s），可自然破冰投放",
            effective_cooldown_sec=eff_cd,
            msg_count_5m=msg_5m,
            human_msgs_today=human_today,
            silence_sec=silence,
            unanswered_streak=st.unanswered_streak,
        )
