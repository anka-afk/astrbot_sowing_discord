"""Tests for the 5-state 'Air Reader' (读空气) ambient FSM and unanswered exponential backoff."""

from __future__ import annotations

from datetime import datetime, timedelta

from ambient.air_reader import AirReader, AirState


def _dt(hour: int, minute: int = 0, day: int = 26) -> datetime:
    return datetime(2026, 11, day, hour, minute, 0)


def test_air_reader_five_states() -> None:
    ar = AirReader(
        default_quiet_hours="01:00-08:00",
        default_cooldown_sec=5400,  # 90 min
        active_chat_threshold_5m=6,
        lull_min_sec=900,  # 15 min
        lull_max_sec=2700,  # 45 min
        dead_group_sec=86400,  # 24h
    )
    g = "aiocqhttp:GroupMessage:12345"

    # 1. QUIET_HOURS (03:30 inside 01:00-08:00)
    res = ar.evaluate(g, now_dt=_dt(3, 30))
    assert res.state == AirState.QUIET_HOURS
    assert res.can_send is False

    # Midnight wrap-around quiet hours (23:30 inside 23:00-07:00)
    res_wrap = ar.evaluate(g, now_dt=_dt(23, 30), quiet_hours="23:00-07:00")
    assert res_wrap.state == AirState.QUIET_HOURS

    # 2. DEAD_GROUP (no human speech ever)
    res_dead = ar.evaluate(g, now_dt=_dt(14, 0))
    assert res_dead.state == AirState.DEAD_GROUP
    assert res_dead.can_send is False

    # 3. ACTIVE_CHAT (>= 6 human messages in last 5 min)
    base = _dt(14, 0)
    for sec in (0, 30, 60, 90, 120, 150):
        ar.record_human_message(g, now_ts=(base + timedelta(seconds=sec)).timestamp())
    res_hot = ar.evaluate(g, now_dt=base + timedelta(minutes=3))
    assert res_hot.state == AirState.ACTIVE_CHAT
    assert res_hot.can_send is False
    assert res_hot.msg_count_5m == 6

    # 4. LULL_READY (20 min after last human message at 14:02:30 -> 14:22:30)
    lull_time = base + timedelta(minutes=22, seconds=30)
    res_lull = ar.evaluate(g, now_dt=lull_time)
    assert res_lull.state == AirState.LULL_READY
    assert res_lull.can_send is True

    # Past 45 min lull window -> DEAD_GROUP (cold)
    res_cold = ar.evaluate(g, now_dt=base + timedelta(minutes=60))
    assert res_cold.state == AirState.DEAD_GROUP
    assert res_cold.can_send is False

    # 5. COOLDOWN right after bot sends a meme
    ar.record_bot_message(g, is_meme=True, now_ts=lull_time.timestamp())
    res_cd = ar.evaluate(g, now_dt=lull_time + timedelta(minutes=30))
    assert res_cd.state == AirState.COOLDOWN
    assert res_cd.can_send is False


def test_air_reader_unanswered_meme_doubles_cooldown_backoff() -> None:
    ar = AirReader(
        default_quiet_hours="01:00-08:00",
        default_cooldown_sec=3600,  # 60 min base
        engagement_window_sec=1800,  # 30 min engagement window
    )
    g = "aiocqhttp:GroupMessage:777"

    t0 = _dt(10, 0)
    ar.record_human_message(g, now_ts=t0.timestamp())

    # Bot sends meme #1 at 10:20
    t_meme1 = _dt(10, 20)
    ar.record_bot_message(g, is_meme=True, now_ts=t_meme1.timestamp())
    # Effective cooldown doubled to 120 min because unanswered_streak == 1
    assert ar.effective_cooldown_sec(g) == 7200

    # Nobody replies -> even 150 min later, last_sender_is_bot blocks self-talk!
    res_monologue = ar.evaluate(g, now_dt=t_meme1 + timedelta(minutes=150))
    assert res_monologue.state == AirState.COOLDOWN
    assert res_monologue.can_send is False

    # Human speaks much later at 11:30 (70 min after meme #1 > 30 min engagement window)
    # -> meme #1 was ignored, so unanswered_streak stays 1 (cooldown remains doubled = 120 min)
    t_late_human = _dt(11, 30)
    ar.record_human_message(g, now_ts=t_late_human.timestamp())
    assert ar.effective_cooldown_sec(g) == 7200

    # At 11:50 (90 min after meme #1: > 60m base cooldown, but < 120m doubled cooldown!)
    res_still_cd = ar.evaluate(g, now_dt=_dt(11, 50))
    assert res_still_cd.state == AirState.COOLDOWN
    assert res_still_cd.can_send is False

    # Human speaks at 12:10, then falls silent 20 min until 12:30 (130 min after meme #1 > 120m)
    ar.record_human_message(g, now_ts=_dt(12, 10).timestamp())
    res_ready = ar.evaluate(g, now_dt=_dt(12, 30))
    assert res_ready.state == AirState.LULL_READY
    assert res_ready.can_send is True

    # Bot sends meme #2 at 12:30 -> human laughs at 12:35 (within 30m engagement window!)
    ar.record_bot_message(g, is_meme=True, now_ts=_dt(12, 30).timestamp())
    ar.record_human_message(g, now_ts=_dt(12, 35).timestamp())
    # Streak resets to 0 -> back to base 60m cooldown!
    assert ar.effective_cooldown_sec(g) == 3600
