# 消息处理
from astrbot.api.event import filter, AstrMessageEvent
from .evaluation.emoji import type1_ids, type2_ids


class MessageHandler:
    def __init__(self, event: AstrMessageEvent):
        self.event = event
        pass

    async def fetch_emoji_like(self, message_id: int, emoji_ids: dict = None):
        """获取消息的各种贴表情数量, 默认获取所有表情数量

        Args:
            message_id (int): 消息id
            emoji_ids (dict): 表情id字典, 键为表情id, 值为表情类型, 可选
        Returns:
            dict: 表情数量字典, 键为表情id, 值为表情数量
        """
        client = self.event.bot
        emoji_count_dict = {}
        if not emoji_ids:
            emoji_ids = {"type1_ids": type1_ids, "type2_ids": type2_ids}
        for id in emoji_ids["type1_ids"]:
            payloads = {"message_id": message_id, "emojiId": id, "emojiType": 1}
            response = await client.api.call_action("fetch_emoji_like", **payloads)
            emojiLikesList = response.get("emojiLikesList")
            if emojiLikesList:
                emoji_count_dict[id] = len(emojiLikesList)
            else:
                emoji_count_dict[id] = 0
        for id in emoji_ids["type2_ids"]:
            payloads = {"message_id": message_id, "emojiId": id, "emojiType": 2}
            response = await client.api.call_action("fetch_emoji_like", **payloads)
            emojiLikesList = response.get("emojiLikesList")
            if emojiLikesList:
                emoji_count_dict[id] = len(emojiLikesList)
            else:
                emoji_count_dict[id] = 0
        return emoji_count_dict
