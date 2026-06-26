"""
Sample end-to-end workflow demonstrating the multi-agent orchestration framework.

Pipeline:
  1. DataAgent   → fetch records from source
  2. DataAgent   → transform fetched records
  3. EmailAgent  → notify analyst with results
  4. NotifyAgent → send webhook to downstream system

Run:
  python workflows/sample_workflow.py
"""

import asyncio
import logging
from agents.orchestrator_agent import OrchestratorAgent
from agents.worker_agents.email_agent import EmailAgent
from agents.worker_agents.data_agent import DataAgent
from agents.worker_agents.notification_agent import NotificationAgent
from protocols.message_schema import (
    AgentMessageSchema,
    TaskType,
    Priority,
)
from protocols.fallback_chain import (
    FallbackChainExecutor,
    DeadLetterQueue,
    RetryPolicy,
)

# ── Logging ─────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Agent Setup ─────────────────────────────────────────────────────

def build_orchestrator() -> OrchestratorAgent:
    orchestrator = OrchestratorAgent()
    orchestrator.register(EmailAgent())
    orchestrator.register(DataAgent())
    orchestrator.register(NotificationAgent())
    return orchestrator


def build_executor(orchestrator: OrchestratorAgent) -> FallbackChainExecutor:
    """Wire up registry → FallbackChainExecutor."""
    registry = {
        name: agent.execute_with_fallback
        for name, agent in orchestrator._registry.items()
    }
    return FallbackChainExecutor(
        registry=registry,
        dlq=DeadLetterQueue(),
        retry_policy=RetryPolicy(max_retries=3, base_delay=0.5),
    )


# ── Workflow Steps ───────────────────────────────────────────────────

def step_fetch() -> AgentMessageSchema:
    return AgentMessageSchema(
        sender="orchestrator",
        recipient="data_agent",
        task_type=TaskType.DATA_FETCH,
        priority=Priority.HIGH,
        fallback_chain=["notification_agent"],  # fallback: notify on fetch failure
        payload={
            "operation": "fetch",
            "source": "sales_db",
        },
    )


def step_transform() -> AgentMessageSchema:
    return AgentMessageSchema(
        sender="orchestrator",
        recipient="data_agent",
        task_type=TaskType.DATA_TRANSFORM,
        priority=Priority.HIGH,
        fallback_chain=[],
        payload={
            "operation": "transform",
            "data": [
                {"id": 1, "amount": 500},
                {"id": 2, "amount": 1200},
                {"id": 3, "amount": 340},
            ],
        },
    )


def step_email_notify() -> AgentMessageSchema:
    return AgentMessageSchema(
        sender="orchestrator",
        recipient="email_agent",
        task_type=TaskType.EMAIL,
        priority=Priority.MEDIUM,
        fallback_chain=["notification_agent"],  # fallback: push if email fails
        payload={
            "to": "analyst@company.com",
            "subject": "Daily Sales Pipeline — Completed",
            "body": "Fetch and transform stages completed. Records processed: 3.",
            "cc": ["manager@company.com"],
        },
    )


def step_webhook() -> AgentMessageSchema:
    return AgentMessageSchema(
        sender="orchestrator",
        recipient="notification_agent",
        task_type=TaskType.NOTIFY_WEBHOOK,
        priority=Priority.LOW,
        fallback_chain=[],
        payload={
            "channel": "webhook",
            "recipient": "downstream_system",
            "message": "Pipeline completed successfully.",
            "url": "https://hooks.example.com/pipeline/complete",
        },
    )


# ── Sequential Workflow ──────────────────────────────────────────────

async def run_sequential(executor: FallbackChainExecutor) -> None:
    logger.info("═══ SEQUENTIAL WORKFLOW START ═══")

    steps = [
        step_fetch(),
        step_transform(),
        step_email_notify(),
        step_webhook(),
    ]

    for i, step in enumerate(steps, 1):
        logger.info(f"── Step {i}/{len(steps)}: {step.task_type} → {step.recipient}")
        result = await executor.execute(step)

        if result:
            logger.info(f"   ✅ Success | status={result.status}")
        else:
            logger.error(f"   ❌ Dead-lettered | message_id={step.message_id}")

    logger.info("═══ SEQUENTIAL WORKFLOW END ═══\n")


# ── Parallel Workflow ────────────────────────────────────────────────

async def run_parallel(executor: FallbackChainExecutor) -> None:
    """
    Run independent steps concurrently.
    Email + webhook have no inter-dependency — safe to parallelise.
    """
    logger.info("═══ PARALLEL WORKFLOW START ═══")

    independent_steps = [
        step_email_notify(),
        step_webhook(),
    ]

    tasks = [executor.execute(step) for step in independent_steps]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for step, result in zip(independent_steps, results):
        if isinstance(result, Exception):
            logger.error(f"   ❌ Exception | task={step.task_type} error={result}")
        elif result:
            logger.info(f"   ✅ Success | task={step.task_type} status={result.status}")
        else:
            logger.error(f"   ❌ Dead-lettered | task={step.task_type}")

    logger.info("═══ PARALLEL WORKFLOW END ═══\n")


# ── Failure Simulation ───────────────────────────────────────────────

async def run_failure_simulation(executor: FallbackChainExecutor) -> None:
    """
    Send a message to a non-existent agent to trigger full fallback chain.
    Demonstrates: retry → fallback → dead-letter path.
    """
    logger.info("═══ FAILURE SIMULATION START ═══")

    bad_message = AgentMessageSchema(
        sender="orchestrator",
        recipient="ghost_agent",          # not registered
        task_type=TaskType.DATA_FETCH,
        priority=Priority.CRITICAL,
        fallback_chain=["another_ghost"], # also not registered
        payload={"operation": "fetch", "source": "nowhere"},
    )

    result = await executor.execute(bad_message)

    if result is None:
        logger.info("   ✅ Dead-letter path confirmed — fallback chain exhausted correctly")

    logger.info("═══ FAILURE SIMULATION END ═══\n")


# ── Entry Point ──────────────────────────────────────────────────────

async def main() -> None:
    orchestrator = build_orchestrator()
    executor     = build_executor(orchestrator)

    await run_sequential(executor)
    await run_parallel(executor)
    await run_failure_simulation(executor)


if __name__ == "__main__":
    asyncio.run(main())
