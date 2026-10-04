"""Local stdio MCP server and durable investigate/review/resume/status commands."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

from mcp import Client, StdioServerParameters

from lakebridge_ops.ledger import fingerprint
from lakebridge_ops.model import HTTPModel
from lakebridge_ops.runtime import MCPTools, Runtime
from lakebridge_ops.tools import BusinessTools, create_server


def environment():
    return {name: os.environ[name] for name in ("LB_BRIDGE_URL", "LB_DATASET_ID", "LB_AGENT_TOKEN")}


async def run(args):
    env = environment()
    model = None
    if endpoint := os.environ.get("LB_MODEL_ENDPOINT"):
        model = HTTPModel(
            endpoint, os.environ["LB_MODEL_NAME"], os.environ.get("LB_MODEL_API_KEY", "")
        )
    binding = fingerprint(
        {
            "url": env["LB_BRIDGE_URL"],
            "dataset_id": env["LB_DATASET_ID"],
            "token_fingerprint": hashlib.sha256(env["LB_AGENT_TOKEN"].encode()).hexdigest(),
            "model_endpoint": os.environ.get("LB_MODEL_ENDPOINT"),
            "model_name": os.environ.get("LB_MODEL_NAME"),
        }
    )
    # The MCP child receives only its own agent credential; human/model/DB secrets
    # are not forwarded. MCP SDK supplies its standard non-secret process environment.
    transport = StdioServerParameters(
        command=sys.executable, args=["-m", "lakebridge_ops.cli", "mcp"], env=env
    )
    async with Client(transport, cache=None) as client:
        runtime = Runtime(args.state_dir, MCPTools(client), binding=binding, model=model)
        try:
            if args.command == "status":
                result = await runtime.status(args.task_id)
            elif args.command == "investigate":
                result = await runtime.drive(
                    args.task_id,
                    intent={
                        "question": args.question,
                        "dataset_id": env["LB_DATASET_ID"],
                        "consumer_id": args.consumer,
                        "from_release": args.from_release,
                        "to_release": args.to_release,
                        "tenant_id": args.tenant,
                        "source_version": args.runbook_version,
                        "run_id": args.run_id,
                        "initial": args.initial,
                        "rebuild": args.rebuild,
                    },
                    budget={
                        "model_calls": args.model_calls,
                        "tool_calls": args.tool_calls,
                        "active_ms": args.active_seconds * 1000,
                        "call_timeout_ms": args.call_timeout_seconds * 1000,
                    },
                )
            else:
                decision = None
                if args.stop:
                    decision = {"stop": True}
                elif args.approval_id:
                    if not args.plan_hash:
                        raise ValueError("--plan-hash must match the independently reviewed plan")
                    decision = {"approval_id": args.approval_id, "plan_hash": args.plan_hash}
                updates = json.loads(args.inputs) if args.inputs else None
                result = await runtime.drive(args.task_id, resume=decision, updates=updates)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            runtime.close()
            if model:
                await model.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("artifacts/ops-agent"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("mcp")
    investigate = commands.add_parser("investigate")
    investigate.add_argument("task_id")
    investigate.add_argument("--question", required=True)
    investigate.add_argument("--consumer")
    investigate.add_argument("--from-release")
    investigate.add_argument("--to-release")
    investigate.add_argument("--tenant")
    investigate.add_argument("--runbook-version", default="usage.v1")
    investigate.add_argument("--run-id")
    investigate.add_argument("--initial", action="store_true")
    investigate.add_argument("--rebuild", action="store_true")
    investigate.add_argument("--model-calls", type=int, default=6)
    investigate.add_argument("--tool-calls", type=int, default=12)
    investigate.add_argument("--active-seconds", type=int, default=180)
    investigate.add_argument("--call-timeout-seconds", type=int, default=15)
    commands.add_parser("status").add_argument("task_id")
    resume = commands.add_parser("resume")
    resume.add_argument("task_id")
    resume.add_argument("--approval-id")
    resume.add_argument("--plan-hash")
    resume.add_argument("--stop", action="store_true")
    resume.add_argument("--inputs", help="JSON correction, accepted only while NeedsInput")
    args = parser.parse_args(argv)
    if args.command == "mcp":
        env = environment()
        tools = BusinessTools(env["LB_BRIDGE_URL"], env["LB_AGENT_TOKEN"], env["LB_DATASET_ID"])
        create_server(tools).run(transport="stdio")
    else:
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
