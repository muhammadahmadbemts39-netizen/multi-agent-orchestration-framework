from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
import uuid


# ── Enums ───────────────────────────────────────────────────────────

class TaskType(str, Enum):
    EMAIL        = "email"
    DATA_FETCH   = "data_fetch"
    DATA_TRANSFORM = "data_transform"
    DATA_PERSIST = "data_persist"
    NOTIFY_SMS   = "notify_sms"
    NOTIFY_PUSH  = "notify_push"
    NOTIFY_WEBHOOK = "notify_webhook"
    # Results
    EMAIL_RESULT        = "email_result"
    DATA_RESULT         = "data_result"
    NOTIFICATION_RESULT = "notification_result"


class Priority(int, Enum):
    CRITICAL = 1
    HIGH     = 2
    MEDIUM   = 3
    LOW      = 4
    BATCH    = 5


class MessageStatus(str, Enum):
    PENDING    = "pending"
    IN_FLIGHT  = "in_flight"
    SUCCESS    = "success"
    FAILED     = "failed"
    DEAD_LETTER = "dead_letter"


# ── Core Schema ─────────────────────────────────────────────────────

class AgentMessageSchema(BaseModel):
    """
    Validated inter-agent message contract.
    Use this instead of raw AgentMessage dataclass when
    messages cross service/network boundaries.
    """
    message_id:     str           = Field(default_factory=lambda: str(uuid.uuid4()))
    sender:         str           = Field(..., min_length=1)
    recipient:      str           = Field(..., min_length=1)
    task_type:      TaskType
    payload:        Dict[str, Any] = Field(default_factory=dict)
    priority:       Priority      = Priority.MEDIUM
    retry_count:    int           = Field(default=0, ge=0, le=5)
    fallback_chain: List[str]     = Field(default_factory=list)
    status:         MessageStatus = MessageStatus.PENDING
    correlation_id: Optional[str] = None  # Links related messages in a workflow

    @field_validator("sender", "recipient")
    @classmethod
    def no_whitespace(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Agent names must not contain spaces")
        return v.lower()

    @field_validator("fallback_chain")
    @classmethod
    def chain_excludes_self(cls, v: List[str], info) -> List[str]:
        sender = info.data.get("sender", "")
        if sender in v:
            raise ValueError(f"Fallback chain must not include sender '{sender}'")
        return v

    def mark_inflight(self) -> "AgentMessageSchema":
        return self.model_copy(update={"status": MessageStatus.IN_FLIGHT})

    def mark_success(self) -> "AgentMessageSchema":
        return self.model_copy(update={"status": MessageStatus.SUCCESS})

    def mark_failed(self) -> "AgentMessageSchema":
        return self.model_copy(update={"status": MessageStatus.FAILED})

    def mark_dead_letter(self) -> "AgentMessageSchema":
        return self.model_copy(update={"status": MessageStatus.DEAD_LETTER})

    def next_fallback(self) -> Optional[str]:
        """Pop and return the next fallback agent, or None if chain exhausted."""
        return self.fallback_chain.pop(0) if self.fallback_chain else None

    class Config:
        use_enum_values = False


# ── Payload Schemas (optional strict typing per task) ───────────────

class EmailPayload(BaseModel):
    to:      str
    subject: str
    body:    str
    cc:      List[str] = Field(default_factory=list)


class DataPayload(BaseModel):
    operation: str  # fetch | transform | persist
    source:    Optional[str] = None
    target:    Optional[str] = None
    data:      List[Any]     = Field(default_factory=list)


class NotifyPayload(BaseModel):
    channel:   str  # sms | push | webhook
    recipient: str
    message:   str
    url:       Optional[str] = None  # webhook only
