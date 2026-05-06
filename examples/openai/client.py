from openai import OpenAI


client = OpenAI(
    base_url="http://localhost:9000/v1",
    api_key="agentgate-local",
)

resp = client.chat.completions.create(
    model="qwen",
    messages=[
        {"role": "system", "content": "You are a careful build assistant."},
        {"role": "user", "content": "Say hello through AgentGate."},
    ],
    extra_body={
        "x_agentgate": {
            "session_id": "demo-session",
            "tenant_id": "local-dev",
            "cache_control": {"prefix_hint": "share_max"},
        }
    },
)

print(resp.choices[0].message.content)
