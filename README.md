# AgentGate

**AgentGate 是专为 Agent 工作负载打造的跨框架推理网关。** 它位于
vLLM / SGLang / Ollama / OpenAI 兼容协议 / Anthropic 后端之前，统一协议、
路由、缓存、流式、降级与追踪——让上层的 Agent 应用不必关心某一步具体
是哪个后端在服务。

## 它解决的是真问题：Agent 流量里的结构性浪费

- **同一系统提示重复 prefill**：100 个租户共用一段 system prompt，每次
  请求都重新算一遍 KV——跨实例没人帮你做亲和性。
- **tool_call 已经完整了，decode 还在继续**：流式输出里模型已经吐完
  `tool_call` 的 JSON，但上游还在生成无意义的解释文字，token 和延迟
  双输。
- **多步 Agent 调用是黑盒**：一次 LangGraph 跑完，你不知道哪一步打了
  哪个后端、prefix 命中了多少、为什么降级。
- **路由 / 预算 / 缓存规则散落在代码里**：换一家供应商要改源码，
  没有控制面。

## 三个核心能力（这是 AgentGate 的护城河）

这三个点是引擎语义层的优化，云厂商的 L7 负载均衡 / 通用 LLM 代理做不
了，因为它们不假设你后端是 vLLM 还是 SGLang，也看不到 Agent 的
session 与 trace。

### 1. 跨引擎 Prefix Locality Routing
将带相同前缀的请求黏到同一后端实例，最大化 vLLM APC / SGLang
RadixAttention 的命中率。后端发现是不是支持前缀复用，靠 capability
sheet——同一份路由代码同时优化多种引擎，不是写死的 `if vendor ==
"vllm"`。
👉 [设计文档](./docs/design/prefix-cache.md)

### 2. Streaming Tool-Call Early Stop
在 SSE 流里检测到完整的 `tool_call` 后，立即取消上游 decode。tool 重
负载的 Agent 场景下能直接砍掉一半以上的解码 token——不需要后端配合，
也不会破坏流式语义。
👉 [设计文档](./docs/design/streaming-tool-early-stop.md)

### 3. Agent Trace 模型
不是 request 级的指标，是 session / trace / step 级的最小可观测模型：
trace_id / session_id / step_id / parent_step_id / prefix-hit-tokens /
decode-tokens-saved / fallback-reason。落地为本地 JSONL +
`/debug/trace/{id}`，OTLP-HTTP 导出到 Tempo / Honeycomb / Datadog 任选。
👉 [设计文档](./docs/design/agent-trace.md)

## AgentGate 在哪一层？vs 其他「AI 网关」

> 「AI 网关」这个词被云厂商和开源项目卷烂了。先把层次讲清楚，再讲
> 谁做什么。

```
                ┌────────────────────────────────────────┐
                │  Agent 编排框架 (LangGraph / AutoGen)  │
                └──────────────────┬─────────────────────┘
                                   │
                ┌──────────────────▼─────────────────────┐
                │  AgentGate（你应用旁边的 Go 二进制）   │
                │  · 跨引擎 prefix locality              │
                │  · streaming tool-call early stop      │
                │  · agent session/trace                 │
                │  · 三层 semantic cache                 │
                │  · 策略引擎 / 熔断 / 成本路由          │
                └──────────────────┬─────────────────────┘
                                   │
       ┌────────────┬──────────────┼──────────────┬────────────┐
       ▼            ▼              ▼              ▼            ▼
    vLLM        SGLang          Ollama       OpenAI 协议   Anthropic
    (APC)    (RadixAttn)                     (DeepSeek/    Messages
                                              Moonshot)
```

|                              | 阿里云 ALB 扩展版                | LiteLLM / Portkey         | vLLM Router / SGLang Router | **AgentGate**                  |
|------------------------------|----------------------------------|---------------------------|-----------------------------|--------------------------------|
| 部署形态                     | 云上托管 L7 LB                   | Python lib / SaaS         | 引擎自带的简单路由器        | 单二进制，进程旁部署            |
| 抽象层次                     | 网络层 + AI 插件壳               | 单次请求级代理            | 单引擎内多实例分发          | **Agent session/trace 级**     |
| 跨引擎 prefix 亲和           | ❌（GPU 感知 ≠ KV 复用）         | ❌                        | ❌（只在自家引擎内）        | ✅ vLLM APC + SGLang RadixAttn |
| streaming tool-call early stop | ❌                             | ❌                        | ❌                          | ✅                             |
| Agent trace（多步 / 父子步） | ❌                               | request 级日志            | ❌                          | ✅ OTLP + JSONL replay         |
| Anthropic Messages 原生      | ❌                               | ✅                        | ❌                          | ✅                             |
| 策略 DSL（路由/缓存/预算）   | 插件 + Callouts                  | YAML 路由                 | ❌                          | ✅ 一份 YAML 三件事             |
| 部署假设                     | 必须在阿里云                     | Python 进程内             | 必须用对应引擎              | 无云依赖，单二进制              |
| 开源 / 厂商锁定              | 闭源云产品                       | 开源（OSS）/ SaaS         | 开源                        | Apache 2.0，无云依赖            |

**一句话总结**：

- ALB 扩展版是 **L7 入口**（你 Nginx/Envoy 那一层的替代），擅长流量
  管理、证书、WAF；它的 "AI 原生" = 多模型代理 + GPU 感知 + Token 限
  速。它不知道你后端是 vLLM 还是 SGLang，**做不了引擎语义层的优化**。
- LiteLLM / Portkey 是 **请求级代理**，统一 SDK 接口，对 Agent 多步
  和 KV 复用没有概念。
- vLLM Router / SGLang Router 是**引擎自带**的简单分发，绑死自家引擎，
  不解决跨引擎 / 跨厂商问题。
- AgentGate 是 **Agent 推理网关**，做的是上面三家都不做的引擎语义层
  优化。可以架在 ALB 后面、跑在 LiteLLM 旁边——它们不互斥。

## 项目状态

- **v0.1** — 最小可用的跨框架 Agent 网关：✅ 已完成
- **v0.2** — 跨框架抽象沉淀：✅ 已完成
- **v1.0** — 控制面：✅ 已完成

详见下方 [Roadmap](#roadmap)。

## 盒子里有什么

**协议层**
- OpenAI 兼容的 `POST /v1/chat/completions`（JSON + SSE 流式）
- `x_agentgate` 扩展字段，承载 session / tenant / agent / trace 标识
  与 `cache_control` 提示

**后端**（capability 驱动，详见 [docs/design/capability-registry.md](./docs/design/capability-registry.md)）
- vLLM（深度集成：多实例、APC 感知、abort、健康探测）
- SGLang（RadixAttention 前缀模式以独立 capability 暴露）
- Ollama（浅集成：用来验证抽象不会泄漏 vLLM 假设）
- OpenAI 协议云（OpenAI / DeepSeek / Moonshot / Together / Fireworks
  ——同一份代码路径，只换 `vendor` + `cost`）
- Anthropic Messages API（独立协议——适配器处理 system/tools 拆分、
  `tool_use` content blocks、event-named SSE）
- Mock（无后端开发模式）

**Agent 感知优化策略**
- Prefix Locality Routing：把共享前缀的请求黏到同一后端实例，最大化
  APC / RadixAttention 命中率
- Streaming Tool-Call Early Stop：流中检测到完整 `tool_call` 即取消
  上游 decode
- 安全优先的 Semantic Cache：确定性的精确匹配 + tool-result + single
  flight 合流，向量层暂留作未来扩展（[设计](./docs/design/semantic-cache.md)）

**控制面**
- 声明式策略 YAML：路由 / 缓存 / 预算，按类别 first-match
  （[设计](./docs/design/policy-engine.md)，[示例](./configs/policy.example.yaml)）
- 每后端熔断 + 多后端降级链（[设计](./docs/design/fallback-and-circuit-breaker.md)）
- 成本感知路由作为 tie-breaker（永远不覆盖 prefix 亲和性）
  （[设计](./docs/design/cost-aware-routing.md)）

**可观测性**
- Agent Trace 最小模型（trace_id / session_id / step_id /
  parent_step_id / prefix-hit-tokens / decode-tokens-saved /
  fallback-reason）——本地 JSONL + `/debug/trace/{id}`
- 从磁盘 replay trace：`/debug/trace/{id}/replay`
- OTLP-HTTP 导出到任意标准 collector（Tempo、Honeycomb、Datadog）——
  无 SDK 依赖，约 150 行从零实现
- 管理端点：`/admin/backends`、`/admin/capabilities`、
  `/admin/prefix/stats`、`/admin/prefix/topk`、`/admin/cache/stats`、
  `/admin/breakers`、`/admin/cost`、`/admin/policy/budgets`

**分布式**
- 通过 `Mirror` 接口插拔分布式前缀存储；进程内 `LocalMirror` 默认随包
  发，Redis 适配器以 recipe 形式提供，让没有该依赖的项目仍保持干净
  （[接口](./internal/cache/prefix/mirror.go)）

**Benchmark 框架** —— `cmd/benchmark` 跑 S2（共享 system prompt）、
S3（5 轮 ReAct）、S4（tool 重负载）三种 workload，支持按 feature 消融
对比，输出 TTFT / 总耗时 / prefix-tokens / early-stop 率分布。

## 快速开始

```bash
make test
make run
```

```bash
curl http://localhost:9000/health
```

```bash
curl http://localhost:9000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "mock",
    "messages": [
      {"role": "system", "content": "You are a careful build assistant."},
      {"role": "user", "content": "hello"}
    ],
    "x_agentgate": {
      "session_id": "demo-session",
      "tenant_id": "local-dev",
      "cache_control": {"prefix_hint": "share_max"}
    }
  }'
```

完全相同的第二次请求会从缓存返回（注意响应头 `X-AgentGate-Cache:
exact`）。trace ID 也在响应头里，可以用以下方式拉到多步 trace：

```bash
curl http://localhost:9000/debug/trace/<trace_id>
```

更多示例见 `examples/openai/client.py`（原生 OpenAI 客户端）和
`examples/langgraph/agent.py`（LangGraph ReAct）。

## 接入真实后端

编辑 `configs/agentgate.example.yaml`：

```yaml
backends:
  - name: vllm-prod
    type: vllm
    endpoints:
      - http://localhost:8000
      - http://localhost:8001

  - name: openai-cloud
    type: openai
    api_key: ${OPENAI_API_KEY}
    cost:
      input_usd_per_1k: 0.00015
      output_usd_per_1k: 0.0006

policy:
  path: configs/policy.example.yaml

telemetry:
  otlp:
    endpoint: http://otel-collector:4318/v1/traces
```

## Benchmark

预置场景：

```bash
go run ./cmd/benchmark \
  -gateway http://localhost:9000/v1/chat/completions \
  -model mock \
  -scenarios S2,S3,S4 \
  -iterations 200 -concurrency 16 \
  -out report.json
```

场景说明：S2 = 100 个租户共用一段 system prompt（前缀亲和性测试）；
S3 = 5 轮 ReAct（跨轮前缀复用）；S4 = tool 重负载（early-stop 测试）。
report 包含每个场景的 P50/P95/P99 TTFT、总延迟、prefix 命中 token 数和
early-stop 率。

**README 里没有任何编造的百分比涨幅。** 按 [BACKGROUND.md §11](./BACKGROUND.md)
所述，真实数字只在 benchmark 报告里给出，包含原始数据和负面用例。

## API

| 端点                                      | 用途                              |
|-------------------------------------------|-----------------------------------|
| `GET  /health`                            | 存活探测                          |
| `GET  /v1/models`                         | 聚合模型列表                      |
| `POST /v1/chat/completions`               | chat（JSON + SSE）                |
| `GET  /admin/backends`                    | 每后端健康状态 + 计数器           |
| `GET  /admin/capabilities`                | 能力清单                          |
| `GET  /admin/prefix/stats`                | 前缀缓存总量                      |
| `GET  /admin/prefix/topk`                 | 最热前缀节点                      |
| `GET  /admin/cache/stats`                 | semantic cache 命中/未命中        |
| `GET  /admin/breakers`                    | 熔断器状态                        |
| `GET  /admin/cost`                        | 每后端成本 EWMA                   |
| `GET  /admin/policy/budgets`              | 预算桶快照                        |
| `GET  /debug/trace/{id}`                  | 内存中 trace                      |
| `GET  /debug/trace/{id}/replay`           | 从 JSONL replay 出来的 trace      |

## 设计原则

- **不引入重依赖。** `go.mod` 里只有一个外部依赖（`yaml.v3`）。OTLP、
  Redis 风格的 mirror、singleflight 全部手写。代价是几百行专注的代
  码，收益是一个小巧、可审计的项目。
- **行为由 capability sheet 驱动，不由 vendor 名字驱动。** 路由和缓存
  代码从不分支判断 `vendor == "vllm"`，只读 `caps.PrefixCacheMode`、
  `caps.SupportsAbort` 等字段。新接一个后端 = 新加一份 sheet = 不动其
  他代码。
- **流式有特殊规则。** 我们绝不缓存流式响应、绝不在流中途降级、绝不
  静默 abort 上游。任何一条都会让客户端遇到难以排查的诡异行为。

## 文档

- [背景与价值](./BACKGROUND.md)
- [架构与模块布局](./ARCHITECTURE.md)
- [架构概览（TL;DR）](./docs/architecture-overview.md)
- [Walkthrough](./docs/walkthrough.md)
- 设计文档（`docs/design/`）
  - [Capability Registry](./docs/design/capability-registry.md)（v0.2）
  - [Prefix Locality Routing](./docs/design/prefix-cache.md)（v0.1）
  - [Streaming Tool Call Early Stop](./docs/design/streaming-tool-early-stop.md)（v0.1）
  - [vLLM Backend Adapter](./docs/design/vllm-adapter.md)（v0.1）
  - [Agent Trace 模型](./docs/design/agent-trace.md)（v0.1）
  - [Semantic Cache](./docs/design/semantic-cache.md)（v0.2）
  - [Policy Engine](./docs/design/policy-engine.md)（v1.0）
  - [Fallback + Circuit Breaker](./docs/design/fallback-and-circuit-breaker.md)（v1.0）
  - [Cost-Aware Routing](./docs/design/cost-aware-routing.md)（v1.0）

## Roadmap

**v0.1** —— 最小可用的跨框架 Agent 网关

- [x] 核心网关 + OpenAI 兼容 chat completions
- [x] capability 驱动的 Backend 接口
- [x] vLLM 适配器（深度）
- [x] Prefix 亲和路由
- [x] Streaming tool-call early stop
- [x] Mock 后端和测试
- [x] Agent Trace 最小模型
- [x] Ollama 浅适配器（验证抽象不泄漏）
- [x] Benchmark 框架（`cmd/benchmark`）

**v0.2** —— 跨框架抽象沉淀

- [x] Capability Registry 提为独立模块
- [x] SGLang 适配器（RadixAttention 提示）
- [x] OpenAI 协议云适配器（OpenAI / DeepSeek 等通用）
- [x] Anthropic Messages API 适配器
- [x] 三层 semantic cache（精确 / tool-result / singleflight）
- [x] OpenTelemetry OTLP-HTTP 导出器

**v1.0** —— 控制面

- [x] 声明式策略引擎（路由 / 缓存 / 预算 DSL）
- [x] 熔断器 + 多后端降级链
- [x] 成本感知路由 + 反馈 EWMA
- [x] 可插拔的分布式前缀存储 mirror
- [x] Trace replay 端点 + LangGraph 示例

**v1.0 之后**（尚未启动）

- [ ] semantic cache 的向量层（需要先确立诚实的相似度门槛 + benchmark
      数据集）
- [ ] 通过 SIGHUP 热重载策略
- [ ] Hedged requests
- [ ] LMCache / vLLM disaggregated-prefill 集成为 `external_kv`
      capability 模式

## License

Apache 2.0
