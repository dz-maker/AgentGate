"""ReAct-style LangGraph agent that talks to an AgentGate gateway.

The point of the example is not LangGraph itself — it is showing that an
unchanged Agent app inherits AgentGate's optimisations purely by setting
``base_url`` and an ``x_agentgate`` extension block.

Each call carries:

  - ``session_id`` so the gateway can correlate spans across the loop
  - ``trace_id`` so the operator can pull the multi-step trace from
    ``GET /debug/trace/{trace_id}`` after the run
  - ``cache_control.prefix_hint = share_max`` so the prefix tracker
    treats the system prompt + tool defs as shareable across runs

The agent itself is intentionally tiny: one tool, three turns. Real
LangGraph workloads see the largest savings when the same system prompt
+ tool definitions are reused across many sessions; this example is
just showing the wiring.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent


def now_in_city(city: str) -> str:
    """A deterministic tool, so its result tier in AgentGate's semantic
    cache stays valid across calls."""
    return json.dumps({"city": city, "time": "2026-05-03T10:00:00+08:00"})


def main() -> None:
    trace_id = "trace_" + uuid.uuid4().hex[:16]
    session_id = "demo-langgraph-" + uuid.uuid4().hex[:8]

    print(f"trace_id={trace_id}")

    llm = ChatOpenAI(
        base_url=os.environ.get("AGENTGATE_URL", "http://localhost:9000/v1"),
        api_key=os.environ.get("AGENTGATE_KEY", "agentgate-local"),
        model=os.environ.get("AGENTGATE_MODEL", "qwen"),
        # x_agentgate is forwarded through ChatOpenAI's extra_body kwargs.
        extra_body={
            "x_agentgate": {
                "session_id": session_id,
                "trace_id": trace_id,
                "agent_id": "langgraph-react-demo",
                "tenant_id": "local-dev",
                "cache_control": {"prefix_hint": "share_max"},
            }
        },
    )

    tools = [now_in_city]
    agent = create_react_agent(llm, tools)

    response: dict[str, Any] = agent.invoke(
        {
            "messages": [
                HumanMessage(
                    content=(
                        "What time is it in Beijing? Use the tool, then "
                        "say hello to the user."
                    ),
                ),
            ],
        }
    )
    last = response["messages"][-1].content
    print("\nfinal answer:", last)
    print(
        f"\ninspect: curl http://localhost:9000/debug/trace/{trace_id}"
    )


if __name__ == "__main__":
    main()
