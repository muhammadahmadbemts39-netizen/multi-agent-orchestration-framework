from __future__ import annotations
import asyncio
import logging
from typing import Callable, Dict, List, Optional, Awaitable
from protocols.message_schema import AgentMessageSchema, MessageStatus

logger = logging.getLogger(__name__)


# ── Retry Config ────────────────────────────────────────────────────

class RetryPolicy:
    """
    Exponential backoff retry policy.
    Default: 3 retries, 0.5s base delay, 2x multiplier, 8s cap.
    """
    def __init__(
        self,
        max_retries:  int   = 3,
        base_delay:   float = 0.5,
        multiplier:   float = 2.0,
        max_delay:    float = 8.0,
    ):
        self.max_retries = max_retries
        self.base_delay  = base_delay
        self.multiplier  = multiplier
        self.max_delay   = max_delay

    def delay_for(self, attempt: int) -> float:
        """Return backoff delay (seconds) for a given attempt number."""
        delay = self.base_delay * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


# ── Dead-Letter Handler ─────────────────────────────────────────────

class DeadLetterQueue:
    """
    In-memory DLQ. Replace `_persist` with Redis/DB write in production.
    """
    def __init__(self):
        self._queue: List[Dict] = []

    def push(self, message: AgentMessageSchema, reason: str) -> None:
        entry = {
            "message_id":  message.message_id,
            "sender":      message.sender,
            "recipient":   message.recipient,
            "task_type":   message.task_type,
            "retry_count": message.retry_count,
            "reason":      reason,
        }
        self._queue.append(entry)
        self._persist(entry)

    def drain(self) -> List[Dict]:
        """Return all DLQ entries and clear the queue."""
        items, self._queue = self._queue, []
        return items

    def __len__(self) -> int:
        return len(self._queue)

    def _persist(self, entry: Dict) -> None:
        # TODO: replace with Redis LPUSH or DB insert
        logger.error(
            f"[DLQ] message_id={entry['message_id']} "
            f"task={entry['task_type']} reason={entry['reason']}"
        )


# ── Fallback Chain Executor ─────────────────────────────────────────

AgentHandler = Callable[[AgentMessageSchema], Awaitable[Optional[AgentMessageSchema]]]


class FallbackChainExecutor:
    """
    Executes a message through:
      Stage 1 → Retry with exponential backoff
      Stage 2 → Route to fallback agents in chain order
      Stage 3 → Dead-letter if all fallbacks exhausted

    Usage:
        executor = FallbackChainExecutor(registry, dlq, retry_policy)
        result   = await executor.execute(message)
    """

    def __init__(
        self,
        registry:     Dict[str, AgentHandler],
        dlq:          DeadLetterQueue,
        retry_policy: RetryPolicy = RetryPolicy(),
    ):
        self._registry     = registry
        self._dlq          = dlq
        self._retry_policy = retry_policy

    # ── Public Entry Point ──────────────────────────────────────────

    async def execute(
        self,
        message: AgentMessageSchema,
    ) -> Optional[AgentMessageSchema]:
        """
        Full fallback chain execution for a single message.
        Returns result message on success, None on dead-letter.
        """
        message = message.mark_inflight()

        # Stage 1: Retry primary agent
        result = await self._retry(message)
        if result is not None:
            return result.mark_success()

        # Stage 2: Walk fallback chain
        result = await self._walk_fallback_chain(message)
        if result is not None:
            return result.mark_success()

        # Stage 3: Dead-letter
        self._dead_letter(message, reason="All retries and fallbacks exhausted")
        return None

    # ── Stage 1: Retry ──────────────────────────────────────────────

    async def _retry(
        self,
        message: AgentMessageSchema,
    ) -> Optional[AgentMessageSchema]:

        handler = self._registry.get(message.recipient)
        if not handler:
            logger.warning(f"[FALLBACK] No handler for '{message.recipient}'")
            return None

        for attempt in range(self._retry_policy.max_retries):
            try:
                logger.info(
                    f"[RETRY] attempt={attempt + 1}/{self._retry_policy.max_retries} "
                    f"message_id={message.message_id}"
                )
                return await handler(message)

            except Exception as e:
                delay = self._retry_policy.delay_for(attempt)
                logger.warning(
                    f"[RETRY] Failed attempt={attempt + 1} "
                    f"error={e} | backing off {delay:.1f}s"
                )
                await asyncio.sleep(delay)

        return None

    # ── Stage 2: Fallback Chain ─────────────────────────────────────

    async def _walk_fallback_chain(
        self,
        message: AgentMessageSchema,
    ) -> Optional[AgentMessageSchema]:

        chain = list(message.fallback_chain)  # non-destructive copy

        for fallback_agent in chain:
            handler = self._registry.get(fallback_agent)
            if not handler:
                logger.warning(f"[FALLBACK] Agent '{fallback_agent}' not in registry — skipping")
                continue

            try:
                logger.info(f"[FALLBACK] Routing to '{fallback_agent}' | message_id={message.message_id}")
                fallback_msg = message.model_copy(
                    update={"recipient": fallback_agent, "retry_count": 0}
                )
                return await handler(fallback_msg)

            except Exception as e:
                logger.warning(f"[FALLBACK] '{fallback_agent}' failed: {e}")
                continue

        return None

    # ── Stage 3: Dead-Letter ────────────────────────────────────────

    def _dead_letter(self, message: AgentMessageSchema, reason: str) -> None:
        updated = message.mark_dead_letter()
        self._dlq.push(updated, reason=reason)
        logger.error(
            f"[DEAD-LETTER] message_id={message.message_id} "
