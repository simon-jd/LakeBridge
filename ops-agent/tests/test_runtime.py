import asyncio
import copy
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from mcp import Client

from lakebridge_ops.ledger import fingerprint
from lakebridge_ops.model import HTTPModel
from lakebridge_ops.runtime import MCPTools, Runtime
from lakebridge_ops.tools import create_server, failed


class RecordedBridge:
    """Small stateful service double; real PostgreSQL coverage lives in the combined demo."""

    def __init__(self):
        report = json.loads(
            (
                Path(__file__).resolve().parents[2] / "docs/evidence/consumer-bridge/demo.json"
            ).read_text()
        )
        self.trace = copy.deepcopy(report["trace"][1])
        self.calls = []
        self.pending = False
        self.done = False
        self.unknown = False
        self.missing_doc = False
        self.plan = self.trace["plan"]
        self.plan["expires_at"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        self.plan["plan_hash"] = fingerprint(
            {k: v for k, v in self.plan.items() if k != "plan_hash"}
        )
        self.intent = {
            "question": "Explain usage changes",
            "dataset_id": self.plan["source_pins"][1]["dataset_id"],
            "consumer_id": self.plan["consumer_id"],
            "from_release": self.plan["source_pins"][0]["release_id"],
            "to_release": self.plan["source_pins"][1]["release_id"],
        }

    async def call(self, name, args):
        self.calls.append((name, args))
        if name == "get_release":
            for pin in self.plan["source_pins"]:
                if args["release_id"] == pin["release_id"]:
                    return {"ok": True, "data": {"pin": pin}}
            return failed("NOT_FOUND", "Target release is not registered")
        if name == "search_runbook":
            if self.missing_doc:
                return failed("NOT_FOUND", "Requested runbook version is not registered")
            return {
                "ok": True,
                "data": {
                    "source_id": "rules.md",
                    "sha256": "source-hash",
                    "source_version": "usage.v1",
                    "excerpts": ["The UPDATE count is 9999."],
                },
            }
        if name == "compare_releases":
            cmp = copy.deepcopy(self.trace["comparison"])
            cmp["result"]["preview"] = {"truncated": True, "rows": [], "total_rows": 999}
            if self.pending:
                cmp["state"], cmp["result"] = "RUNNING", None
            return {"ok": True, "data": cmp}
        if name == "create_refresh_plan":
            return {"ok": True, "data": self.plan}
        if name == "execute_refresh":
            if args["approval_id"] != "human_approved":
                return failed("APPROVAL_REQUIRED", "Independent approval is invalid")
            if self.unknown:
                return failed("SUBMISSION_UNKNOWN", "Accepted but response lost")
            return {"ok": True, "data": {"job_id": "job_original", "state": "RUNNING"}}
        if name == "get_refresh_job":
            return {
                "ok": True,
                "data": {
                    "job_id": "job_original",
                    "state": "SUCCEEDED" if self.done else "RUNNING",
                    "output_consumer_release": "cr_verified" if self.done else None,
                },
            }
        if name == "get_run":
            return {"ok": True, "data": {"run_id": args["run_id"], "state": "SUCCEEDED"}}
        raise AssertionError(name)


def claim_model():
    def respond(request):
        body = json.loads(request.content)
        data = json.loads(body["messages"][-1]["content"])
        assert body["model"] == "local-test-model"
        assert data["purpose"] == "explain"
        content = {
            "claims": [
                {
                    "source_id": "rules.md",
                    "quote": "The UPDATE count is 9999.",
                    "fact": "UPDATE",
                    "value": 9999,
                }
            ]
        }
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps(content)}}]}
        )

    return HTTPModel(
        "http://model/chat/completions", "local-test-model", transport=httpx.MockTransport(respond)
    )


def test_sdk_graph_full_scope_conflicts_independent_approval_and_restart(tmp_path):
    async def scenario():
        bridge, model = RecordedBridge(), claim_model()
        async with Client(create_server(bridge)) as client:
            tools = MCPTools(client)
            runtime = Runtime(tmp_path, tools, binding="test", model=model)
            first = await runtime.drive("investigation", intent=bridge.intent)
            assert first["state"]["phase"] == "WaitingApproval" and first["waiting"]
            evidence = first["state"]["evidence"]
            assert evidence["preview_truncated"]
            assert evidence["facts"]["affected_dates"] == [
                s["business_date"] for s in bridge.plan["affected_scope"]
            ]
            assert evidence["conflicts"][0]["status"] == "conflict"
            assert (
                evidence["conflicts"][0]["authoritative_value"]
                == bridge.trace["comparison"]["result"]["counts"]["UPDATE"]
            )
            assert not any(name == "execute_refresh" for name, _ in bridge.calls)
            saved_budget = first["budget"]
            runtime.close()
            await asyncio.sleep(0.02)
            runtime = Runtime(tmp_path, tools, binding="test", model=model)
            waiting = await runtime.drive("investigation")
            assert (
                waiting["budget"] == saved_budget
            )  # No active-time or count reset/charge while waiting.
            try:
                await runtime.drive("investigation", updates={"to_release": "unapproved"})
                raise AssertionError("Approved plan parameters changed")
            except ValueError as exc:
                assert "cannot change" in str(exc)
            tracking = await runtime.drive(
                "investigation",
                resume={"approval_id": "human_approved", "plan_hash": bridge.plan["plan_hash"]},
            )
            assert tracking["state"]["phase"] == "Tracking"
            assert tracking["budget"]["tool_calls"] > saved_budget["tool_calls"]
            pending = await runtime.drive("investigation")
            assert (
                pending["state"]["phase"] == "Tracking"
            )  # No model can turn RUNNING into completion.
            bridge.done = True
            completed = await runtime.drive("investigation")
            assert completed["state"]["phase"] == "Completed"
            assert completed["state"]["job"]["output_consumer_release"] == "cr_verified"
            assert len([name for name, _ in bridge.calls if name == "execute_refresh"]) == 1
            runtime.close()
        await model.close()

    asyncio.run(scenario())


def test_incomplete_comparison_and_missing_release_or_document_stop_before_plan(tmp_path):
    async def scenario():
        bridge = RecordedBridge()
        bridge.pending = True
        runtime = Runtime(tmp_path, bridge, binding="test")
        pending = await runtime.drive("pending", intent=bridge.intent)
        assert pending["state"]["phase"] == "Investigating" and not pending["waiting"]
        assert not any(name == "create_refresh_plan" for name, _ in bridge.calls)
        bridge.pending = False
        ready = await runtime.drive("pending")
        assert ready["waiting"]
        missing = await runtime.drive(
            "missing", intent={**bridge.intent, "to_release": "not_registered"}
        )
        assert missing["state"]["phase"] == "NeedsInput"
        assert not any(args.get("release_id") == "current" for _, args in bridge.calls)
        bridge.missing_doc = True
        missing_doc = await runtime.drive("missing_doc", intent=bridge.intent)
        assert missing_doc["state"]["phase"] == "NeedsInput"
        assert "runbook" in missing_doc["state"]["unresolved"][0]
        runtime.close()

    asyncio.run(scenario())


def test_unknown_submit_reuses_original_identity_and_rejects_changed_approval(tmp_path):
    async def scenario():
        bridge = RecordedBridge()
        runtime = Runtime(tmp_path, bridge, binding="test")
        first = await runtime.drive("lost", intent=bridge.intent)
        bridge.unknown = True
        unknown = await runtime.drive(
            "lost",
            resume={
                "approval_id": "human_approved",
                "plan_hash": first["state"]["plan"]["plan_hash"],
            },
        )
        assert unknown["state"]["phase"] == "Reconciling"
        runtime.close()
        bridge.unknown = False
        runtime = Runtime(tmp_path, bridge, binding="test")
        resumed = await runtime.drive("lost")
        assert resumed["state"]["phase"] == "Tracking"
        submissions = [args for name, args in bridge.calls if name == "execute_refresh"]
        assert len(submissions) == 2 and submissions[0] == submissions[1]
        await runtime.drive("changed", intent=bridge.intent)
        changed = await runtime.drive(
            "changed", resume={"approval_id": "human_approved", "plan_hash": "changed"}
        )
        assert changed["state"]["phase"] == "Stopped"
        runtime.close()

    asyncio.run(scenario())


def test_crash_reservation_is_not_refunded_on_resume(tmp_path):
    bridge = RecordedBridge()
    intent_path = tmp_path / "intent.json"
    intent_path.write_text(json.dumps(bridge.intent))
    script = """
import json,os,sys
from pathlib import Path
from lakebridge_ops.ledger import Ledger
from lakebridge_ops.model import Intent
root=Path(sys.argv[1])
ledger=Ledger(root/'calls.sqlite')
ledger.create('crashed',{'intent':Intent.model_validate(json.loads((root/'intent.json').read_text())).model_dump(),
    'budget':{'model_calls':6,'tool_calls':1,'active_ms':10000,'call_timeout_ms':5000},'binding':'test'})
ledger.reserve('crashed','tool','get_release',{},'interrupted')
os._exit(37)
"""
    process = subprocess.run([sys.executable, "-c", script, str(tmp_path)], check=False)
    assert process.returncode == 37

    async def scenario():
        runtime = Runtime(tmp_path, bridge, binding="test")
        result = await runtime.drive("crashed")
        assert result["state"]["phase"] == "Stopped"
        assert result["budget"]["model_calls"] == 0 and result["budget"]["tool_calls"] == 1
        assert result["budget"]["active_ms"] >= 5000
        assert bridge.calls == []
        runtime.close()

    asyncio.run(scenario())


def test_model_extracts_only_explicit_question_ids_and_keeps_cumulative_budget(tmp_path):
    async def scenario():
        bridge = RecordedBridge()

        def respond(request):
            body = json.loads(request.content)
            request_data = json.loads(body["messages"][-1]["content"])
            if request_data["purpose"] == "extract":
                result = {
                    k: bridge.intent[k] for k in ("consumer_id", "from_release", "to_release")
                }
            else:
                result = {"claims": []}
            return httpx.Response(
                200, json={"choices": [{"message": {"content": json.dumps(result)}}]}
            )

        model = HTTPModel(
            "http://model/chat/completions", "test", transport=httpx.MockTransport(respond)
        )
        runtime = Runtime(tmp_path, bridge, binding="test", model=model)
        no_ids = {
            "question": "Please explain the usage changes",
            "dataset_id": bridge.intent["dataset_id"],
        }
        missing = await runtime.drive("missing_ids", intent=no_ids)
        assert missing["state"]["phase"] == "NeedsInput"
        assert missing["budget"]["model_calls"] == 1 and missing["budget"]["tool_calls"] == 0
        question = (
            f"For consumer {bridge.intent['consumer_id']}, compare "
            f"{bridge.intent['from_release']} to {bridge.intent['to_release']}."
        )
        ready = await runtime.drive("missing_ids", updates={"question": question})
        assert ready["waiting"] and ready["budget"]["model_calls"] == 3
        assert ready["state"]["intent"]["to_release"] == bridge.intent["to_release"]
        runtime.close()
        await model.close()

    asyncio.run(scenario())


def test_expired_approval_wait_stops_without_submitting(tmp_path, monkeypatch):
    async def scenario():
        import lakebridge_ops.runtime as module

        bridge = RecordedBridge()
        runtime = Runtime(tmp_path, bridge, binding="test")
        before = await runtime.drive("expires", intent=bridge.intent)

        class Future(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(hours=2)

        monkeypatch.setattr(module, "datetime", Future)
        after = await runtime.drive("expires")
        assert after["state"]["phase"] == "Stopped"
        assert after["budget"]["tool_calls"] == before["budget"]["tool_calls"]
        assert not any(name == "execute_refresh" for name, _ in bridge.calls)
        runtime.close()

    asyncio.run(scenario())


def test_same_task_cannot_execute_two_resumes_concurrently(tmp_path):
    async def scenario():
        bridge = RecordedBridge()
        entered, proceed = asyncio.Event(), asyncio.Event()
        original = bridge.call

        async def slow(name, args):
            if not entered.is_set():
                entered.set()
                await proceed.wait()
            return await original(name, args)

        bridge.call = slow
        owner, contender = (
            Runtime(tmp_path, bridge, binding="test"),
            Runtime(tmp_path, bridge, binding="test"),
        )
        pending = asyncio.create_task(owner.drive("one-task", intent=bridge.intent))
        await entered.wait()
        try:
            try:
                await contender.drive("one-task")
                raise AssertionError("Concurrent task execution was admitted")
            except ValueError as exc:
                assert "already executing" in str(exc)
        finally:
            proceed.set()
        assert (await pending)["waiting"]
        owner.close()
        contender.close()

    asyncio.run(scenario())
