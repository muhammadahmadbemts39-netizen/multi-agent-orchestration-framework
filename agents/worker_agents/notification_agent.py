import asyncio
from typing import Optional
from agents.base_agent import BaseAgent, AgentMessage


class NotificationAgent(BaseAgent):
    """
    Handles push/SMS/webhook notifications.
    Expects payload: { channel: sms|push|webhook, recipient, message }
    """

    SUPPORTED_CHANNELS = ("sms", "push", "webhook")

    def __init__(self):
        super().__init__(name="notification_agent")

    async def handle(self, message: AgentMessage) -> Optional[AgentMessage]:
        payload = message.payload
        channel = payload.get("channel")

        if channel not in self.SUPPORTED_CHANNELS:
            raise ValueError(
                f"[NOTIFY] Unsupported channel: '{channel}'. "
                f"Must be one of {self.SUPPORTED_CHANNELS}"
            )

        await self._send(channel, payload)

        return AgentMessage(
            task_type="notification_result",
            payload={"status": "delivered", "channel": channel},
            sender=self.name,
            recipient=message.sender,
        )

    async def _send(self, channel: str, payload: dict) -> None:
        await asyncio.sleep(0.05)  # Simulate dispatch latency

        recipient = payload.get("recipient", "unknown")
        msg = payload.get("message", "")

        if channel == "sms":
            print(f"[NOTIFY/SMS] → {recipient}: '{msg}'")
        elif channel == "push":
            print(f"[NOTIFY/PUSH] → {recipient}: '{msg}'")
        elif channel == "webhook":
            url = payload.get("url", "")
            print(f"[NOTIFY/WEBHOOK] POST → {url} | payload_size={len(str(payload))}B")
