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
import re
from datetime import datetime, time as dtime


@register("astrbot_sowing_discord", "anka", "anka - 搬史插件", "0.915")
class Sowing_Discord(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.instance_id = str(uuid.uuid4())[:8]

        self.banshi_interval = config.get("banshi_interval", 3600)
        self.banshi_cache_seconds = config.get("banshi_cache_seconds", 3600)
        self.cooldown_day_seconds = config.get("banshi_cooldown_day_seconds", 600)
        self.cooldown_night_seconds = config.get("banshi_cooldown_night_seconds", 3600)
        self.cooldown_day_start_str = config.get("banshi_cooldown_day_start", "09:00")
        self.cooldown_night_start_str = config.get("banshi_cooldown_night_start", "01:00")
        
        self._day_start = self._parse_time_str(self.cooldown_day_start_str, dtime(9, 0))
        self._night_start = self._parse_time_str(self.cooldown_night_start_str, dtime(1, 0))

        self.banshi_group_list = config.get("banshi_group_list", [])
        self.banshi_target_list = config.get("banshi_target_list", [])
        self.block_source_messages = config.get("block_source_messages", False)
        
        self.allowed_msg_types = config.get("allowed_message_types", ["text", "image", "video", "forward"])
        
        self.local_cache = LocalCache(max_age_seconds=self.banshi_cache_seconds)
        self.forward_lock = asyncio.Lock()
        self._forward_task = None

    def _parse_time_str(self, time_str: str, fallback: dtime) -> dtime:
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
        now = datetime.now().time()
        if now >= self._day_start or now < self._night_start:
            return self.cooldown_day_seconds
        return self.cooldown_night_seconds

    # 【修复与优化】完美兼容 AstrBot 的对象格式与 OneBot API 的字典格式
    def _is_allowed_msg_type(self, message) -> bool:
        if not self.allowed_msg_types:
            return False

        allowed = set(self.allowed_msg_types)
        found_types = set()

        if isinstance(message, list):
            for seg in message:
                if isinstance(seg, dict):
                    # 情况 A: 处理底层 OneBot API 返回的 dict 格式（转发复核时使用）
                    mtype = seg.get("type", "")
                    if mtype == "image": found_types.add("image")
                    elif mtype == "video": found_types.add("video")
                    elif mtype in ["forward", "node"]: found_types.add("forward")
                    elif mtype in ["text", "face", "at", "reply"]:
                        if mtype == "text" and not seg.get("data", {}).get("text", "").strip():
                            continue
                        found_types.add("text")
                else:
                    # 情况 B: 处理 AstrBot 框架传递过来的 Component 对象（源群入库前验证时使用）
                    cname = seg.__class__.__name__.lower()
                    if cname == "image": found_types.add("image")
                    elif cname == "video": found_types.add("video")
                    elif cname in ["forward", "node"]: found_types.add("forward")
                    elif cname in ["plain", "text", "face", "at", "reply"]:
                        # 对象的文本属性一般是 text
                        if cname in ["plain", "text"] and not getattr(seg, "text", "").strip():
                            continue
                        found_types.add("text")

        elif isinstance(message, str):
            # 情况 C: 处理纯 CQ码 字符串备用兜底
            if "[CQ:image" in message: found_types.add("image")
            if "[CQ:video" in message: found_types.add("video")
            if "[CQ:forward" in message or "[CQ:node" in message: found_types.add("forward")
            
            text_only = re.sub(r'\[CQ:.*?\]', '', message).strip()
            if text_only:
                found_types.add("text")

        # 规则1: 如果消息中包含了用户【未授权】的核心媒体类型，拦截整条消息。
        for t in ["image", "video", "forward"]:
            if t in found_types and t not in allowed:
                return False

        # 规则2: 如果消息没有任何符合用户设定项的元素，拦截。
        if not found_types.intersection(allowed):
            return False

        return True

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    async def handle_message(self, event: AstrMessageEvent):
        forward_manager = ForwardManager(event)
        evaluator = Evaluator(event)
        evaluator.add_rule(GoodEmojiRule())

        source_group_id = event.message_obj.group_id
        msg_id = event.message_obj.message_id
        
        # 将群号统一转为字符串进行判定，防止因格式不同（int/str）导致匹配失败
        is_in_source_list = str(source_group_id) in [str(g) for g in self.banshi_group_list]

        # 【核心修复】如果是源群消息，且开启了屏蔽源群选项，立即拦截事件，阻止大模型和下游插件响应！
        if is_in_source_list and self.block_source_messages:
            event.stop_event()

        sender_id = event.get_sender_id()

        if not self.banshi_target_list:
            self.banshi_target_list = await self.get_group_list(event)

        if is_in_source_list:
            # 兼容获取消息结构
            raw_msg = getattr(event.message_obj, 'message', getattr(event.message_obj, 'raw_message', ""))
            if not self._is_allowed_msg_type(raw_msg):
                logger.debug(f"[SowingDiscord][ID:{self.instance_id}] 预拦截：消息 (ID: {msg_id}) 包含未被允许的类型。")
            else:
                try:
                    int(msg_id)
                    await self.local_cache.add_cache(msg_id)
                    logger.info(
                        f"[SowingDiscord][ID:{self.instance_id}] 任务：缓存。已缓存消息 (ID: {msg_id}, 源头群: {source_group_id}, 发送者: {sender_id})。"
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        f"[SowingDiscord][ID:{self.instance_id}] 拦截异常：消息 ID [{msg_id}] 不是有效的纯数字形式，跳过缓存。"
                    )

        try:
            waiting_messages = await self.local_cache.get_waiting_messages()
        except ValueError as e:
            logger.error(
                f"[SowingDiscord][ID:{self.instance_id}] 缓存严重损坏！遇到了无法转换为数字的旧数据：{e}。请进入插件目录删掉损坏的缓存文件！"
            )
            waiting_messages = []

        if waiting_messages:
            if not self.forward_lock.locked():
                # 【优化】将其作为后台任务运行，避免冷却期的 sleep 卡死事件循环，导致其他消息无响应
                asyncio.create_task(self._execute_forward_and_cool(event, forward_manager, evaluator))

        return None

    async def _execute_forward_and_cool(
        self, event, forward_manager, evaluator
    ):
        client = event.bot

        try:
            current_task = asyncio.current_task()
            self._forward_task = current_task
            cleaned_count = await self.local_cache._cleanup_expired_cache()
            if cleaned_count > 0:
                logger.info(
                    f"[SowingDiscord][ID:{self.instance_id}] 转发前自动清理了 {cleaned_count} 条超出最大缓存时长的消息。"
                )

            try:
                # 重新获取最新的待转发列表
                waiting_messages = await self.local_cache.get_waiting_messages()
            except ValueError:
                waiting_messages = []

            async with self.forward_lock:
                logger.info(
                    f"[SowingDiscord][ID:{self.instance_id}] 执行任务：转发。检测到 {len(waiting_messages)} 条待转发消息，开始处理..."
                )

                for index, msg_id_to_forward in enumerate(waiting_messages):
                    earliest_timestamp_limit = time.time() - self.banshi_cache_seconds
                    target_list_str = ", ".join(map(str, self.banshi_target_list))

                    try:
                        message_detail = await client.api.call_action(
                            "get_msg", message_id=int(msg_id_to_forward)
                        )
                        message_time = message_detail.get("time", 0)
                        msg_content = message_detail.get("message", [])

                        if message_time < earliest_timestamp_limit:
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            continue

                        if not msg_content:
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            continue
                            
                        # API 拿到的是字典形式，进行二次类型校验
                        if not self._is_allowed_msg_type(msg_content):
                            logger.info(
                                f"[SowingDiscord] 预检查失败：消息ID {msg_id_to_forward} 包含未被配置允许的消息类型。"
                            )
                            await self.local_cache.remove_cache(msg_id_to_forward)
                            continue

                    except ActionFailed:
                        await self.local_cache.remove_cache(msg_id_to_forward)
                        continue

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
                            
                            interval = self._get_banshi_interval_dynamic()
                            self._forward_task = current_task
                            logger.info(
                                f"[SowingDiscord][ID:{self.instance_id}] 冷却开始：时长 {interval} 秒 (持有锁)。"
                            )
                            await asyncio.sleep(interval)
                            self._forward_task = None 

                        else:
                            await self.local_cache.remove_cache(msg_id_to_forward)

                    except ActionFailed as e:
                        logger.error(
                            f"[SowingDiscord][ID:{self.instance_id}] 转发失败 (API 拒绝)：消息 ID {msg_id_to_forward} ... 原因: {e}"
                        )
                        await self.local_cache.remove_cache(msg_id_to_forward)
                        continue
                    except asyncio.CancelledError:
                        self._forward_task = None
                        raise

            logger.info(
                f"[SowingDiscord][ID:{self.instance_id}] 本次所有待转发消息处理完毕，释放转发锁。"
            )
        except asyncio.CancelledError:
            pass
        finally:
            self._forward_task = None

    # 【修复退出报错】添加了 async，防止重启框架/重载插件时由于阻塞报错
    async def terminate(self):
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
        return group_ids