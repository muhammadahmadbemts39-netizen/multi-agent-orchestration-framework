"""
Unit tests for the multi-agent orchestration framework.

Coverage:
  - BaseAgent fallback mechanics
  - OrchestratorAgent routing + registration
  - FallbackChainExecutor (retry → fallback → dead-letter)
  - MessageSchema validation
  - All 3 worker agents (happy path + error path)

Run:
  pytest tests/ -v
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from agents.base_agent import BaseAgent, AgentMessage
from agents.orchestrator_agent import OrchestratorAgent
from agents.worker_agents.email_agent import EmailAgent
from agents.worker_agents.data_agent import DataAgent
from agents.worker_agents.notification_agent import NotificationAgent
from protocols.message_schema import (
    AgentMessageSchema,
    TaskType,
    Priority,
    MessageStatus,
)
from protocols.fallback_chain import (
    FallbackChainExecutor,
    DeadLetterQueue,
    RetryPolicy,
)


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def orchestrator():
    o = OrchestratorAgent()
    o.register(EmailAgent())
    o.register(DataAgent())
    o.register(NotificationAgent())
    return o


@pytest.fixture
def dlq():
    return DeadLetterQueue()


@pytest.fixture
def retry_policy():
    return RetryPolicy(max_retries=2, base_delay=0.01)  # fast for tests


@pytest.fixture
def executor(orchestrator, dlq, retry_policy):
    registry = {
        name: agent.execute_with_fallback
        for name, agent in orchestrator._registry.items()
    }
    return FallbackChainExecutor(
        registry=registry,
        dlq=dlq,
        retry_policy=retry_policy,
    )


def make_schema(**kwargs) -> AgentMessageSchema:
    defaults = dict(
        sender="orchestrator",
        recipient="data_agent",
        task_type=TaskType.DATA_FETCH,
        priority=Priority.MEDIUM,
        payload={"operation": "fetch", "source": "test_db"},
        fallback_chain=[],
    )
    defaults.update(kwargs)
    return AgentMessageSchema(**defaults)


# ── MessageSchema Validation ─────────────────────────────────────────

class TestMessageSchema:

    def test_valid_message_creates_uuid(self):
        msg = make_schema()
        assert msg.message_id is not None
        assert len(msg.message_id) == 36  # UUID4 format

    def test_sender_whitespace_rejected(self):
        with pytest.raises(ValueError, match="must not contain spaces"):
            make_schema(sender="bad agent")

    def test_sender_in_fallback_chain_rejected(self):
        with pytest.raises(ValueError, match="must not include sender"):
            make_schema(sender="orchestrator", fallback_chain=["orchestrator"])

    def test_status_transitions(self):
        msg = make_schema()
        assert msg.status == MessageStatus.PENDING
        assert msg.mark_inflight().status  == MessageStatus.IN_FLIGHT
        assert msg.mark_success().status   == MessageStatus.SUCCESS
        assert msg.mark_failed().status    == MessageStatus.FAILED
        assert msg.mark_dead_letter().status == MessageStatus.DEAD_LETTER

    def test_next_fallback_pops_chain(self):
        msg = make_schema(fallback_chain=["agent_b", "agent_c"])
        assert msg.next_fallback() == "agent_b"
        assert msg.fallback_chain == ["agent_c"]

    def test_next_fallback_returns_none_when_empty(self):
        msg = make_schema(fallback_chain=[])
        assert msg.next_fallback() is None


# ── OrchestratorAgent ────────────────────────────────────────────────

class TestOrchestratorAgent:

    def test_register_agent(self, orchestrator):
        assert "email_agent" in orchestrator.registered_agents()
        assert "data_agent" in orchestrator.registered_agents()
        assert "notification_agent" in orchestrator.registered_agents()

    @pytest.mark.asyncio
    async def test_routes_to_correct_agent(self, orchestrator):
        msg = AgentMessage(
            task_type="data_fetch",
            payload={"operation": "fetch", "source": "db"},
            sender="orchestrator",
            recipient="data_agent",
        )
        result = await orchestrator.handle(msg)
        assert result is not None
        assert result.sender == "data_agent"

    @pytest.mark.asyncio
    async def test_unregistered_agent_raises(self, orchestrator):
        msg = AgentMessage(
            task_type="data_fetch",
            payload={},
            sender="orchestrator",
            recipient="ghost_agent",
        )
        with pytest.raises(ValueError, match="No agent registered"):
            await orchestrator.handle(msg)

    @pytest.mark.asyncio
    async def test_run_workflow_sequential(self, orchestrator):
        steps = [
            AgentMessage(
                task_type="data_fetch",
                payload={"operation": "fetch", "source": "db"},
                sender="orchestrator",
                recipient="data_agent",
            ),
            AgentMessage(
                task_type="email",
                payload={"to": "a@b.com", "subject": "Test", "body": "Hello"},
                sender="orchestrator",
                recipient="email_agent",
            ),
        ]
        results = await orchestrator.run_workflow(steps)
        assert len(results) == 2
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    async def test_run_parallel(self, orchestrator):
        steps = [
            AgentMessage(
                task_type="notify_sms",
                payload={"channel": "sms", "recipient": "+92300", "message": "Hi"},
                sender="orchestrator",
                recipient="notification_agent",
            ),
            AgentMessage(
                task_type="email",
                payload={"to": "x@y.com", "subject": "S", "body": "B"},
                sender="orchestrator",
                recipient="email_agent",
            ),
        ]
        results = await orchestrator.run_parallel(steps)
        assert len(results) == 2


# ── Worker Agents ────────────────────────────────────────────────────

class TestEmailAgent:

    @pytest.mark.asyncio
    async def test_happy_path(self):
        agent = EmailAgent()
        msg = AgentMessage(
            task_type="email",
            payload={"to": "a@b.com", "subject": "Hi", "body": "Test"},
            sender="orchestrator",
            recipient="email_agent",
        )
        result = await agent.handle(msg)
        assert result.payload["status"] == "sent"
        assert result.payload["to"] == "a@b.com"

    @pytest.mark.asyncio
    async def test_missing_payload_raises(self):
        agent = EmailAgent()
        msg = AgentMessage(
            task_type="email",
            payload={"to": "a@b.com"},  # missing subject + body
            sender="orchestrator",
            recipient="email_agent",
        )
        with pytest.raises(ValueError, match="Missing payload fields"):
            await agent.handle(msg)


class TestDataAgent:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["fetch", "transform", "persist"])
    async def test_all_operations(self, operation):
        agent = DataAgent()
        msg = AgentMessage(
            task_type=f"data_{operation}",
            payload={"operation": operation, "data": [], "source": "db", "target": "db"},
            sender="orchestrator",
            recipient="data_agent",
        )
        result = await agent.handle(msg)
        assert result.payload["status"] == "ok"
        assert result.payload["operation"] == operation

    @pytest.mark.asyncio
    async def test_invalid_operation_raises(self):
        agent = DataAgent()
        msg = AgentMessage(
            task_type="data_fetch",
            payload={"operation": "delete"},  # unsupported
            sender="orchestrator",
            recipient="data_agent",
        )
        with pytest.raises(ValueError, match="Unsupported operation"):
            await agent.handle(msg)


class TestNotificationAgent:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("channel", ["sms", "push", "webhook"])
    async def test_all_channels(self, channel):
        agent = NotificationAgent()
        msg = AgentMessage(
            task_type=f"notify_{channel}",
            payload={
                "channel": channel,
                "recipient": "user_1",
                "message": "Test",
                "url": "https://hook.example.com",
            },
            sender="orchestrator",
            recipient="notification_agent",
        )
        result = await agent.handle(msg)
        assert result.payload["status"] == "delivered"
        assert result.payload["channel"] == channel

    @pytest.mark.asyncio
    async def test_invalid_channel_raises(self):
        agent = NotificationAgent()
        msg = AgentMessage(
            task_type="notify_sms",
            payload={"channel": "fax", "recipient": "user", "message": "Hi"},
            sender="orchestrator",
            recipient="notification_agent",
        )
        with pytest.raises(ValueError, match="Unsupported channel"):
            await agent.handle(msg)


# ── FallbackChainExecutor ────────────────────────────────────────────

class TestFallbackChainExecutor:

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self, executor):
        msg = make_schema(
            recipient="data_agent",
            payload={"operation": "fetch", "source": "db"},
        )
        result = await executor.execute(msg)
        assert result is not None
        assert result.status == MessageStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_dead_letter_on_missing_agent(self, executor, dlq):
        msg = make_schema(
            recipient="ghost_agent",
            fallback_chain=["another_ghost"],
        )
        result = await executor.execute(msg)
        assert result is None
        assert len(dlq) == 1

    @pytest.mark.asyncio
    async def test_fallback_agent_used_on_primary_failure(self, dlq, retry_policy):
        """Primary always fails → fallback agent (data_agent) succeeds."""

        async def always_fails(msg):
            raise RuntimeError("Simulated failure")

        registry = {
            "failing_agent": always_fails,
            "data_agent": DataAgent().execute_with_fallback,
        }
        ex = FallbackChainExecutor(registry, dlq, retry_policy)

        msg = make_schema(
            recipient="failing_agent",
            fallback_chain=["data_agent"],
            payload={"operation": "fetch", "source": "db"},
        )
        result = await ex.execute(msg)
        assert result is not None
        assert result.status == MessageStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_retry_count_exhausted_goes_to_dlq(self, dlq):
        """All retries fail, no fallback → dead-letter."""

        async def always_fails(msg):
            raise RuntimeError("Always fails")

        registry = {"bad_agent": always_fails}
        ex = FallbackChainExecutor(
            registry, dlq,
            RetryPolicy(max_retries=2, base_delay=0.01),
        )
        msg = make_schema(recipient="bad_agent", fallback_chain=[])
        result = await ex.execute(msg)
        assert result is None
        assert len(dlq) == 1


# ── DeadLetterQueue ──────────────────────────────────────────────────

class TestDeadLetterQueue:

    def test_push_and_len(self):
        dlq = DeadLetterQueue()
        msg = make_schema()
        dlq.push(msg, reason="test")
        assert len(dlq) == 1

    def test_drain_clears_queue(self):
        dlq = DeadLetterQueue()
        msg = make_schema()
        dlq.push(msg, reason="test")
        items = dlq.drain()
        assert len(items) == 1
        assert len(dlq) == 0

    def test_drain_entry_fields(self):
        dlq = DeadLetterQueue()
        msg = make_schema(sender="orchestrator", recipient="data_agent")
        dlq.push(msg, reason="exhausted")
        entry = dlq.drain()[0]
        assert entry["reason"]    == "exhausted"
        assert entry["sender"]    == "orchestrator"
        assert entry["recipient"] == "data_agent"
