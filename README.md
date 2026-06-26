# Multi-Agent Orchestration Framework

A modular framework for orchestrating specialised AI agents in business workflow automation pipelines.

## Architecture

```
┌─────────────────────────────────────────┐
│           Orchestrator Agent            │
│  (Task routing + fallback chain mgmt)  │
└────────────┬────────────────────────────┘
             │ Message Protocol (JSON schema)
    ┌────────┼─────────┐
    ▼        ▼         ▼
[Email   [Data     [Notification
 Agent]   Agent]    Agent]
    │        │         │
    └────────┴─────────┘
         Error-Recovery
         Fallback Chain
```

## Features

- **Modular agent design** — each agent handles a single business domain
- **Inter-agent communication protocol** — typed message schemas with validation
- **Error-recovery fallback chains** — automatic retry → fallback agent → dead-letter queue
- **Pluggable workflow engine** — define workflows as DAGs via config

## Quickstart

```bash
pip install -r requirements.txt
python workflows/sample_workflow.py
```

## Agent Communication Protocol

Each agent communicates via a typed `AgentMessage` schema:

```python
{
  "message_id": "uuid",
  "sender": "agent_name",
  "recipient": "agent_name | broadcast",
  "task_type": "email | data | notify",
  "payload": {},
  "priority": 1-5,
  "retry_count": 0,
  "fallback_chain": ["agent_b", "agent_c"]
}
```

## Error Recovery

| Stage | Trigger | Action |
|-------|---------|--------|
| Retry | Transient failure | Re-queue with backoff (3x) |
| Fallback | Retry exhausted | Route to next agent in chain |
| Dead-letter | All fallbacks fail | Log + alert + manual queue |

## Tech Stack

- Python 3.11
- `asyncio` for non-blocking agent execution
- `pydantic` for message schema validation
- `redis` for agent message queue
- `pytest` for agent unit tests
