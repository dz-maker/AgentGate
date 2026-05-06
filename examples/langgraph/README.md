# LangGraph + AgentGate

这个示例演示如何把一个 LangGraph ReAct agent 指向 AgentGate 网关，
让它**不改任何 Agent 代码**就继承 prefix-locality 路由、流式 tool-call
早停、semantic cache 和端到端 Agent Trace 可观测性。

"裸 OpenAI" 和 "通过 AgentGate" 之间，唯一的差别是 `base_url` 和
`extra_body.x_agentgate` 这一段。

## 运行

1. 用示例配置启动 AgentGate：

   ```bash
   make run
   ```

2. 安装 LangGraph（建议单独建 venv，避免和 gateway 依赖冲突）：

   ```bash
   pip install langgraph langchain-openai
   ```

3. 跑 agent：

   ```bash
   python agent.py
   ```

4. 看它生成的 trace：

   ```bash
   curl http://localhost:9000/debug/trace/<trace_id>
   ```

   把 `<trace_id>` 替换成 `agent.py` 打出来的那个值。LangGraph 跑里
   的每一步 LLM 调用都会变成一条 span；sticky-prefix 命中和
   tool-call 早停节省的 token 数都能在 span 上看到。

## 为什么这事有意义

LangGraph 把 LLM 调用当成黑盒——它负责工具调用的串接和重新 prompt，
**但不优化这些调用本身**。把 LLM 调用代理给 AgentGate 后，同一份
LangGraph 代码就自动享有：

  - 跨多步 ReAct 循环的 prefix 亲和性复用
  - tool_call 完整缓冲后立即早停 decode
  - 跨 run 共享的、租户隔离的 semantic cache
  - 一条覆盖整个 run 所有 LLM hop 的统一 trace_id

可以直接通过切换 `base_url`（在 `http://localhost:9000/v1` 和上游
provider 之间），做对照。
