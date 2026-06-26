from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
import uuid

@dataclass
class AgentMessage:
    task_type: str
    payload: dict
    sender: str
    recipient: str
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: int = 3
    retry_count: int = 0
    fallback_chain: List[str] = field(default_factory=list)

class BaseAgent(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def handle(self, message: AgentMessage) -> Optional[AgentMessage]:
        pass

    async def execute_with_fallback(self, message: AgentMessage):
        try:
            return await self.handle(message)
        except Exception as e:
            return await self._trigger_fallback(message, error=e)

    async def _trigger_fallback(self, message: AgentMessage, error: Exception):
        if message.retry_count < 3:
            message.retry_count += 1
            return await self.execute_with_fallback(message)
        if message.fallback_chain:
            next_agent = message.fallback_chain.pop(0)
            print(f"[FALLBACK] Routing to {next_agent} after {error}")
            # In real impl: dispatch to next_agent via queue
        else:
            print(f"[DEAD-LETTER] {message.message_id} — all fallbacks exhausted")
