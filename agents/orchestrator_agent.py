import asyncio
from typing import Dict, List, Optional
from agents.base_agent import BaseAgent, AgentMessage


class OrchestratorAgent(BaseAgent):
    """
    Central coordinator. Routes messages to registered agents,
    manages execution order, and supervises fallback chains.
    """

    def __init__(self):
        super().__init__(name="orchestrator")
        self._registry: Dict[str, BaseAgent] = {}
        self._workflow_queue: asyncio.Queue = asyncio.Queue()

    # ── Agent Registration ──────────────────────────────────────────

    def register(self, agent: BaseAgent) -> None:
        """Register a worker agent by name."""
        self._registry[agent.name] = agent
        print(f"[REGISTRY] Agent registered: {agent.name}")

    def registered_agents(self) -> List[str]:
        return list(self._registry.keys())

    # ── Core Routing ────────────────────────────────────────────────

    async def handle(self, message: AgentMessage) -> Optional[AgentMessage]:
        """Route a single message to the correct agent."""
        target = self._registry.get(message.recipient)

        if not target:
            raise ValueError(
                f"[ORCHESTRATOR] No agent registered for: '{message.recipient}'. "
                f"Available: {self.registered_agents()}"
            )

        print(f"[ROUTE] {message.sender} → {message.recipient} | task={message.task_type}")
        return await target.execute_with_fallback(message)

    # ── Workflow Execution ──────────────────────────────────────────

    async def run_workflow(self, steps: List[AgentMessage]) -> List[Optional[AgentMessage]]:
        """
        Execute a list of AgentMessages as an ordered workflow.
        Each step runs sequentially; failures trigger fallback chains.
        """
        results = []
        for step in steps:
            result = await self.execute_with_fallback(step)
            results.append(result)
        return results

    async def run_parallel(self, steps: List[AgentMessage]) -> List[Optional[AgentMessage]]:
        """
        Execute workflow steps concurrently (no inter-step dependency).
        Use when steps are independent of each other.
        """
        tasks = [self.execute_with_fallback(step) for step in steps]
        return await asyncio.gather(*tasks, return_exceptions=True)

    # ── Queue-Based Dispatch (async producer/consumer) ──────────────

    async def enqueue(self, message: AgentMessage) -> None:
        """Push a message onto the internal workflow queue."""
        await self._workflow_queue.put(message)
        print(f"[QUEUE] Enqueued: {message.message_id} | priority={message.priority}")

    async def process_queue(self) -> None:
        """
        Continuously drain the workflow queue.
        Run as a background task: asyncio.create_task(orchestrator.process_queue())
        """
        print("[QUEUE] Processor started...")
        while True:
            message: AgentMessage = await self._workflow_queue.get()
            try:
                await self.handle(message)
            except Exception as e:
                print(f"[ERROR] Failed to process {message.message_id}: {e}")
            finally:
                self._workflow_queue.task_done()
