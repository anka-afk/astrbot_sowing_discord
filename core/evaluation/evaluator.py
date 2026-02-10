# 用于评估一条消息是否应该被转发
from astrbot.api.event import AstrMessageEvent
from typing import List
from .rules import Rule


class Evaluator:
    """消息评估器，可以组合多个规则进行评估"""

    def __init__(self, event: AstrMessageEvent):
        self.event = event
        self.rules: List[Rule] = []

    def add_rule(self, rule: Rule):
        """添加评估规则"""
        self.rules.append(rule)

    async def evaluate(self, message_id: int) -> bool:
        """根据所有规则评估消息

        Returns:
            bool: 如果所有规则都通过返回 True，否则返回 False
        """
        for rule in self.rules:
            if not await rule.evaluate(self.event, message_id):
                return False
        return True
