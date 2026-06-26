import asyncio
from typing import Optional
from agents.base_agent import BaseAgent, AgentMessage


class EmailAgent(BaseAgent):
    """
    Handles email dispatch tasks.
    Expects payload: { to, subject, body }
    """

    def __init__(self):
        super().__init__(name="email_agent")

    async def handle(self, message: AgentMessage) -> Optional[AgentMessage]:
        payload = message.payload

        # Validate required fields
        missing = [f for f in ("to", "subject", "body") if f not in payload]
        if missing:
            raise ValueError(f"[EMAIL] Missing payload fields: {missing}")

        # Simulate send (replace with SMTP / SendGrid call)
        await asyncio.sleep(0.1)
        print(f"[EMAIL] Sent → {payload['to']} | subject='{payload['subject']}'")

        return AgentMessage(
            task_type="email_result",
            payload={"status": "sent", "to": payload["to"]},
            sender=self.name,
            recipient=message.sender,
        )
