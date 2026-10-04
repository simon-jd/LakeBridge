# LakeBridge Ops Agent

Independent MCP/LangGraph companion for investigating pinned release changes and
proposing a downstream refresh. It imports no LakeBridge, Spark, Arrow or database
drivers. Its only business access is the authenticated Consumer Bridge HTTP API.

The official MCP SDK exposes seven typed tools: `get_release`, `get_run`,
`search_runbook`, `compare_releases`, `create_refresh_plan`, `execute_refresh`, and
`get_refresh_job`. There is no approval tool, SQL executor or filesystem tool.
The CLI starts a real stdio MCP child with only the agent's service credentials.
The loopback service uses configured distinct static tokens; remote MCP/OAuth is
outside this local MVP.

```bash
uv sync --project ops-agent --frozen --extra dev --python 3.11
export LB_BRIDGE_URL=http://127.0.0.1:8765
export LB_DATASET_ID=saas_seed_42
export LB_AGENT_TOKEN="$LAKEBRIDGE_AGENT_TOKEN"
ops-agent/.venv/bin/lakebridge-ops --state-dir artifacts/ops-agent investigate usage-1 \
  --question 'Investigate t0000 changes from rel_demo_1 to rel_demo_2' \
  --consumer usage --from-release rel_demo_1 --to-release rel_demo_2 --tenant t0000
ops-agent/.venv/bin/lakebridge-ops --state-dir artifacts/ops-agent resume usage-1
```

Run the independent Consumer Bridge worker to finish asynchronous comparisons.
The graph persists its full plan, evidence and stable execution identity, then
interrupts at `WaitingApproval`. A human uses the separate service CLI to review
and approve the exact hash (see [Consumer Bridge](../docs/consumer-bridge.md)):

```bash
lakebridge-consumer review PLAN_ID
lakebridge-consumer approve PLAN_ID --plan-hash REVIEWED_HASH --decision approved
ops-agent/.venv/bin/lakebridge-ops --state-dir artifacts/ops-agent resume usage-1 \
  --approval-id APPROVAL_ID --plan-hash REVIEWED_HASH
ops-agent/.venv/bin/lakebridge-ops --state-dir artifacts/ops-agent status usage-1
```

`resume --stop` stops a waiting task. `resume --inputs '{...}'` corrects missing
logical IDs only while `NeedsInput`; it cannot replace a plan waiting for approval.
The service rechecks permission, expiry, baseline, scope and plan identity before
admission and publication. Tracking remains incomplete until a successful job
records its output consumer release. A lost POST response is reconciled by the
original idempotency key before any retry.

SQLite stores LangGraph checkpoints synchronously and a separate cumulative call
ledger. A task defaults to 6 model calls, 12 tool calls, 180 active seconds and a
15-second call timeout. It reserves call/time budget before each external call;
a crashed process leaves that reservation charged. Waiting for a human consumes
no active budget. A per-task process lock excludes simultaneous resumes. Preserve
the state directory and service identity when restarting.

Explicit IDs work without a model. To enable intent extraction and sourced claims,
configure `LB_MODEL_ENDPOINT` (a chat-completions-compatible HTTP endpoint),
`LB_MODEL_NAME` and optionally `LB_MODEL_API_KEY`. Extracted identifiers must occur
literally in the question. Claims require exact runbook quotations/source IDs;
tool facts determine counts, pins and affected dates. Unsupported or conflicting
claims remain visible and cannot change scope or execution state. Model credentials
are never forwarded to the MCP child.

```bash
ops-agent/.venv/bin/pytest -q ops-agent/tests
LAKEBRIDGE_CONSUMER_DSN=... .venv/bin/python scripts/ops_agent_demo.py \
  --source-root artifacts/consumer-demo-20260913 \
  --root artifacts/ops-agent-new --namespace ops_agent_new
```

The demo runs actual stdio MCP, loopback HTTP, PostgreSQL and dbt across separate
Agent invocations; it uses scripted independent human approval and no paid/live
model. The configured HTTP model adapter is covered by deterministic mocked-model
behavioral tests. [Saved evidence](../docs/evidence/ops-agent/verification.json)
records that distinction, approval rejection, checkpoint restart and full target
aggregate parity.

Implementation references: [MCP SDK tools](https://py.sdk.modelcontextprotocol.io/servers/tools/)
and [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence).
Forge's stage/approval design informed the interface; its coding workspace,
Docker executor and verifier were not copied into this companion.
