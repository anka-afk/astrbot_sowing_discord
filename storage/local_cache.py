# local_cache.py

from ..config import TEMP_DIR, WAITING_TIME
import os
import json
import time
import asyncio
from astrbot.api import logger


class LocalCache:
    def __init__(self, max_age_seconds: int = 3600):
        self.cache_file = os.path.join(TEMP_DIR, "local_cache.json")
        self.WAITING_TIME = WAITING_TIME
        self.MAX_CACHE_AGE_SECONDS = max_age_seconds

        self._file_lock = asyncio.Lock()

        cache_dir = os.path.dirname(self.cache_file)
        os.makedirs(cache_dir, exist_ok=True)

        if not os.path.exists(self.cache_file):
            with open(self.cache_file, "w") as f:
                json.dump({}, f)

    async def _cleanup_expired_cache(self) -> int:
        """清理缓存中超过 MAX_CACHE_AGE_SECONDS 的消息，并返回清理数量。"""
        current_time = time.time()
        cleaned_count = 0

        async with self._file_lock:
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)

            except (FileNotFoundError, json.JSONDecodeError):
                logger.error("[LocalCache][CLEANUP] 错误：文件不存在或内容格式错误。")
                return 0

            keys_to_keep = {}
            for message_id_str, timestamp in cache.items():
                if current_time - timestamp > self.MAX_CACHE_AGE_SECONDS:
                    cleaned_count += 1
                else:
                    keys_to_keep[message_id_str] = timestamp

            if cleaned_count > 0:
                with open(self.cache_file, "w") as f:
                    json.dump(keys_to_keep, f)

            return cleaned_count

    async def add_cache(self, message_id: int):
        """添加一条message_id进入缓存, 保存时间"""
        str_message_id = str(message_id)

        async with self._file_lock:
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                cache = {}

            cache[str_message_id] = time.time()

            with open(self.cache_file, "w") as f:
                json.dump(cache, f)

    async def get_waiting_messages(self) -> list:
        """获取已经等待足够时间的消息列表"""

        waiting_messages = []
        current_time = time.time()

        async with self._file_lock:
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)

            except (FileNotFoundError, json.JSONDecodeError):
                return []

        for message_id_str, timestamp in cache.items():
            if current_time - timestamp > self.WAITING_TIME:
                waiting_messages.append(int(message_id_str))

        return waiting_messages

    async def remove_cache(self, message_id: int):
        """转发成功或失败后，手动删除指定的 message_id"""
        str_message_id = str(message_id)

        async with self._file_lock:
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)

            except (FileNotFoundError, json.JSONDecodeError):
                return False

            if str_message_id in cache:
                del cache[str_message_id]

                with open(self.cache_file, "w") as f:
                    json.dump(cache, f)

                return True
            return False
