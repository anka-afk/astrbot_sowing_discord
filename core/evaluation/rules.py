# 用于评价的贴表情规则
from typing import Any, Coroutine
from ..message_handler import MessageHandler
from astrbot.api.event import AstrMessageEvent


class Rule:
    """评价一条转发消息是否应该被转发的抽象基类, 任何具体的规则需要继承此类

    Attributes:
        rule_name (str): 规则名称
    """

    def __init__(self, rule_name: str):
        self.rule_name = rule_name

    async def evaluate(self, message_id: int) -> bool:
        """评价一条转发消息是否应该被转发

        Args:
            message_id (int): 消息id

        Returns:
            bool: 是否应该被转发
        """


class GoodEmojiRule(Rule):
    """评价一条转发消息是否应该被转发的规则, 依据点赞和点踩的数量之差"""

    def __init__(self):
        super().__init__("GoodEmojiRule")
        self.good_emoji_ids = [
            4,
            8,
            12,
            14,
            16,
            21,
            24,
            28,
            29,
            30,
            42,
            43,
            49,
            53,
            60,
            63,
            66,
            74,
            75,
            76,
            78,
            79,
            85,
            89,
            99,
            101,
            109,
            116,
            118,
            122,
            124,
            125,
            129,
            144,
            147,
            171,
            175,
            179,
            180,
            182,
            183,
            201,
            203,
            212,
            214,
            219,
            222,
            227,
            232,
            243,
            246,
            277,
            281,
            282,
            289,
            290,
            293,
            294,
            297,
            298,
            299,
            305,
            306,
            307,
            314,
            315,
            318,
            319,
            320,
            324,
            9728,
            9749,
            9786,
            10024,
            127801,
            127817,
            127822,
            127827,
            127836,
            127838,
            127847,
            127866,
            127867,
            127881,
            128046,
            128051,
            128053,
            128076,
            128077,
            128079,
            128089,
            128102,
            128104,
            128147,
            128157,
            128164,
            128170,
            128235,
            128293,
            128513,
            128514,
            128516,
            128522,
            128524,
            128536,
            128538,
            128540,
            128541,
        ]
        self.bad_emoji_ids = [
            5,
            9,
            10,
            23,
            25,
            26,
            27,
            32,
            33,
            34,
            38,
            39,
            41,
            96,
            97,
            98,
            100,
            102,
            103,
            104,
            106,
            111,
            120,
            123,
            173,
            174,
            176,
            181,
            240,
            262,
            264,
            265,
            266,
            267,
            268,
            270,
            272,
            273,
            278,
            284,
            285,
            287,
            322,
            326,
            10060,
            10068,
            128027,
            128074,
            128166,
            128168,
            128527,
            128530,
            128531,
            128532,
            128557,
            128560,
            128563,
        ]

    async def evaluate(self, event: AstrMessageEvent, message_id: int) -> bool:
        """评价一条转发消息是否应该被转发, 规则: 好表情数量大于坏表情数量

        Args:
            event (AstrMessageEvent): 事件, 用于传递客户端
            message_id (int): 消息id

        Returns:
            bool: 是否应该被转发
        """
        message_handler = MessageHandler(event)
        emoji_count_dict = await message_handler.fetch_emoji_like(message_id)
        good_emoji_count = 0
        bad_emoji_count = 0
        for emoji_id, emoji_count in emoji_count_dict.items():
            if emoji_id in self.good_emoji_ids:
                good_emoji_count += emoji_count
            elif emoji_id in self.bad_emoji_ids:
                bad_emoji_count += emoji_count
        return good_emoji_count >= bad_emoji_count
