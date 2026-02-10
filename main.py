from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api.all import *
from astrbot.api import logger
from aiocqhttp.exceptions import ActionFailed
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from .core.forward_manager import ForwardManager
from .core.evaluation.evaluator import Evaluator
from .core.evaluation.rules import GoodEmojiRule
from .storage.local_cache import LocalCache
import asyncio
import time
import uuid
from datetime import datetime, time as dtime


@register("astrbot_sowing_discord", "anka", "anka - 搬史插件", "0.915")
class Sowing_Discord(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.instance_id = str(uuid.uuid4())[:8]

        # 仍然读取配置中的 banshi_interval 以保持兼容，但实际冷却时将按时间段动态计算
        self.banshi_interval = config.get("banshi_interval", 3600)
        self.banshi_cache_seconds = config.get("banshi_cache_seconds", 3600)
        # 动态冷却配置（可自定义），默认：白天600s，夜间3600s
        self.cooldown_day_seconds = config.get("banshi_cooldown_day_seconds", 600)
        self.cooldown_night_seconds = config.get("banshi_cooldown_night_seconds", 3600)
        # 冷却时间段起始时间（可自定义），默认：白天09:00，夜间01:00
        self.cooldown_day_start_str = config.get("banshi_cooldown_day_start", "09:00")
        self.cooldown_night_start_str = config.get(
            "banshi_cooldown_night_start", "01:00"
        )
        # 解析为 time 对象
        self._day_start = self._parse_time_str(self.cooldown_day_start_str, dtime(9, 0))
        self._night_start = self._parse_time_str(
            self.cooldown_night_start_str, dtime(1, 0)
        )

        self.banshi_group_list = config.get("banshi_group_list")
        self.banshi_target_list = config.get("banshi_target_list")
        self.block_source_messages = config.get("block_source_messages", False)
        self.local_cache = LocalCache(max_age_seconds=self.banshi_cache_seconds)

        self.forward_lock = asyncio.Lock()
        self._forward_task = None

    def _parse_time_str(self, time_str: str, fallback: dtime) -> dtime:
        """解析 HH:MM 字符串为 time 对象，失败时返回 fallback。"""
        try:
            if isinstance(time_str, str):
                parts = time_str.split(":")
                h = int(parts[0])
                m = int(parts[1]) if len(parts) > 1 else 0
                if 0 <= h < 24 and 0 <= m < 60:
                    return dtime(h, m)
        except Exception as e:
            logger.warning(
                f"[SowingDiscord][ID:{self.instance_id}] 冷却时间段解析失败: {time_str}, 使用默认值。错误: {e}"
            )
        return fallback

    def _get_banshi_interval_dynamic(self) -> int:
        """
        根据本地时间动态计算搬史间隔（可配置）：
        - [day_start, 24:00) ∪ [00:00, night_start) => 返回白天冷却秒数
        - [night_start, day_start) => 返回夜间冷却秒数
        注意：时间段跨越午夜。
        """
        now = datetime.now().time()
        if now >= self._day_start or now < self._night_start:
            return self.cooldown_day_seconds
        return self.cooldown_night_seconds

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    async def handle_message(self, event: AstrMessageEvent):
        forward_manager = ForwardManager(event)
        evaluator = Evaluator(event)
        evaluator.add_rule(GoodEmojiRule())

        source_group_id = event.message_obj.group_id
        msg_id = event.message_obj.message_id
        is_in_source_list = source_group_id in self.banshi_group_list

        sender_id = event.get_sender_id()

        if not self.banshi_target_list:
            self.banshi_target_list = await self.get_group_list(event)

        raw_message = event.message_obj.raw_message
        message_list = (
            raw_message.get("message") if isinstance(raw_message, dict) else None
        )
        is_forward = (
            message_list is not None
            and isinstance(message_list, list)
            and message_list
            and isinstance(message_list[0], dict)
            and message_list[0].get("type") == "forward"
        )

        if is_forward and is_in_source_list:
            await self.local_cache.add_cache(msg_id)
            logger.info(
                f"[SowingDiscord][ID:{self.instance_id}] 任务：缓存。已缓存转发消息 (ID: {msg_id}, 源头群: {source_group_id}, 发送者: {sender_id})。"
            )

        waiting_messages = await self.local_cache.get_waiting_messages()

        # 转发/冷却逻辑
        if waiting_messages:
            if self.forward_lock.locked():
                pass
            else:
                await self._execute_forward_and_cool(
                    event, forward_manager, evaluator, waiting_messages
                )

        if self.block_source_messages and is_in_source_list:
            return MessageEventResult(None)

        return None

    async def _execute_forward_and_cool(
        self, event, forward_manager, evaluator, waiting_messages
    ):
        """
        核心转发逻辑，包含对消息内容的预检查（是否被撤回/是否过期）。
        """
        client = event.bot

        try:
            current_task = asyncio.current_task()
            # 记录当前任务以便 terminate 时可取消（无论是否正处于冷却中）
            self._forward_task = current_task
            cleaned_count = await self.local_cache._cleanup_expired_cache()
            if cleaned_count > 0:
                logger.info(
                    f"[SowingDiscord][ID:{self.instance_id}] 转发前自动清理了 {cleaned_count} 条超出最大缓存时长的消息。"
                )

            waiting_messages = await self.local_cache.get_waiting_messages()

            async with self.forward_lock:
                logger.info(
                    f"[SowingDiscord][ID:{self.instance_id}] 执行任务：转发。成功获取转发锁。检测到 {len(waiting_messages)} 条待转发消息，开始处理..."
                )

                for index, msg_id_to_forward in enumerate(waiting_messages):
                    earliest_timestamp_limit = time.time() - self.banshi_cache_seconds

                    target_list_str = ", ".join(map(str, self.banshi_target_list))

                    # === 预检查：是否被撤回或过期 ===
                    try:
                        message_detail = await client.api.call_action(
                            "get_msg", message_id=int(msg_id_to_forward)
                        )
                        message_time = message_detail.get("time", 0)
                        msg_content = message_detail.get("message", [])

                        # 检查: 消息时间是否超出缓存范围
                        if message_time < earliest_timestamp_limit:
                            logger.info(
                                f"[SowingDiscord] 预检查失败：消息ID {msg_id_to_forward} (时间: {time.strftime('%H:%M:%S', time.localtime(message_time))}) 已超出 {self.banshi_cache_seconds} 秒缓存限制。"
                            )
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            continue

                        # 检查: 消息内容是否被撤回
                        if not msg_content:
                            logger.info(
                                f"[SowingDiscord] 预检查失败：消息ID {msg_id_to_forward} 内容为空或复杂类型，判断为已撤回/失效。"
                            )
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            continue

                    except ActionFailed as e:
                        logger.info(
                            f"[SowingDiscord] 预检查API失败：消息ID {msg_id_to_forward} (可能已撤回/不存在)。原因: {e}"
                        )
                        await self.local_cache.remove_cache(msg_id_to_forward)
                        continue

                    # === 预检查通过，开始转发流程 ===
                    start_time_for_cooldown = time.time()
                    try:
                        if await evaluator.evaluate(msg_id_to_forward):
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 转发详情 (No.{index + 1}, ID: {msg_id_to_forward})：目标群列表: [{target_list_str}]。"
                            )

                            for target_id in self.banshi_target_list:
                                await forward_manager.send_forward_msg_raw(
                                    msg_id_to_forward, target_id
                                )
                                logger.info(
                                    f"[SowingDiscord][ID:{self.instance_id}] 发送日志：成功转发消息 (ID: {msg_id_to_forward}) 到目标群: {target_id}。"
                                )
                                await asyncio.sleep(1)

                            await self.local_cache.remove_cache(msg_id_to_forward)
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 缓存清理：消息 (ID: {msg_id_to_forward}) 转发成功，已手动清除缓存。"
                            )

                            # 冷却：根据时间段动态设置间隔
                            interval = self._get_banshi_interval_dynamic()
                            self._forward_task = current_task
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 冷却开始：时长 {interval} 秒 (持有锁)。"
                            )
                            await asyncio.sleep(interval)
                            self._forward_task = None  # 冷却完成后清除跟踪

                            end_time = time.time()
                            actual_duration = end_time - start_time_for_cooldown
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 冷却结束：实际耗时约 {actual_duration:.2f} 秒 (包含发送时间)。"
                            )

                        else:
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 消息 (ID: {msg_id_to_forward}) 评估未通过，跳过转发。"
                            )
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 缓存清理：消息 (ID: {msg_id_to_forward}) 评估失败，已手动清除缓存。"
                            )

                    except ActionFailed as e:
                        logger.error(
                            f"[SowingDiscord][ID:{self.instance_id}] 转发失败 (API 拒绝)：消息 ID {msg_id_to_forward} ... 原因: {e}"
                        )
                        await self.local_cache.remove_cache(msg_id_to_forward)
                        logger.warning(
                            f"[SowingDiscord][ID:{self.instance_id}] 缓存清理：消息 (ID: {msg_id_to_forward}) 因 API 拒绝而失败，已手动清除缓存。继续下一条消息。"
                        )
                        continue
                    except asyncio.CancelledError:
                        logger.warning(
                            f"[SowingDiscord][ID:{self.instance_id}] 任务在冷却期间被强制取消。"
                        )
                        self._forward_task = None
                        raise

            logger.info(
                f"[SowingDiscord][ID:{self.instance_id}] 本次所有待转发消息处理完毕，释放转发锁。"
            )
        except asyncio.CancelledError:
            logger.warning(
                f"[SowingDiscord][ID:{self.instance_id}] 转发任务协程被强制终止，确保锁已释放。"
            )
        finally:
            self._forward_task = None

    def terminate(self):
        """在插件停止时，取消仍在进行的冷却任务，避免阻塞关闭。"""
        try:
            if self._forward_task and not self._forward_task.done():
                logger.info(
                    f"[SowingDiscord][ID:{self.instance_id}] 插件终止：正在取消冷却任务。"
                )
                self._forward_task.cancel()
        except Exception as e:
            logger.error(
                f"[SowingDiscord][ID:{self.instance_id}] 终止时取消任务失败: {e}"
            )

    async def get_group_list(self, event: AstrMessageEvent):
        client = event.bot
        response = await client.api.call_action("get_group_list", no_cache=False)
        group_ids = [item["group_id"] for item in response]
        logger.info(
            f"[SowingDiscord] 目标群列表为空，自动获取到 {len(group_ids)} 个群组作为目标群。"
        )
        return group_ids
