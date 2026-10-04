"""Bounded LangGraph investigation with independent human approval interrupts."""

from __future__ import annotations

import asyncio
import fcntl
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import TypedDict

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from lakebridge_ops.ledger import BudgetExceeded, Ledger, fingerprint
from lakebridge_ops.model import Extracted, Intent, reconcile_claims
from lakebridge_ops.tools import INPUTS, ExecuteInput, failed


class State(TypedDict, total=False):
    task_id: str
    intent: dict
    phase: str
    next_node: str
    releases: dict
    runbook: dict
    run: dict
    comparison: dict
    plan: dict
    evidence: dict
    approval_id: str
    idempotency_key: str
    job: dict
    unresolved: list[str]


class MCPTools:
    def __init__(self, client):
        self.client = client

    async def call(self, name, arguments):
        if name not in INPUTS:
            return failed("FORBIDDEN", "Unregistered tool")
        result = await self.client.call_tool(name, {"request": arguments})
        if result.is_error or not isinstance(result.structured_content, dict):
            return failed("UNAVAILABLE", "MCP returned an invalid structured tool result")
        return result.structured_content


class Runtime:
    def __init__(self, directory: Path, tools, *, binding: str, model=None):
        self.directory, self.tools, self.model, self.binding = directory, tools, model, binding
        directory.mkdir(parents=True, exist_ok=True)
        self.ledger = Ledger(directory / "calls.sqlite")

    def close(self):
        self.ledger.close()

    async def call(self, state, name, arguments, *, kind="tool", cache=False):
        key = fingerprint({"kind": kind, "name": name, "arguments": arguments})
        if cache and (prior := self.ledger.cached(state["task_id"], key)) is not None:
            return prior
        call_id, timeout_ms, started = self.ledger.reserve(
            state["task_id"], kind, name, arguments, key
        )
        try:
            async with asyncio.timeout(timeout_ms / 1000):
                if kind == "model":
                    response = {"ok": True, "data": await self.model.call(name, arguments)}
                else:
                    response = await self.tools.call(name, arguments)
        except Exception:
            response = failed(
                "SUBMISSION_UNKNOWN" if name == "execute_refresh" else "UNAVAILABLE",
                "Call failed or timed out; original task identity is retained",
            )
        self.ledger.finish(call_id, started, response, cache and response.get("ok", False))
        return response

    @staticmethod
    def stop_for_error(response, phase="NeedsInput"):
        error = response.get("error", {})
        return {
            "phase": phase,
            "next_node": "end",
            "unresolved": [f"{error.get('code', 'UNKNOWN')}: {error.get('message', '')}"],
        }

    async def identify(self, state):
        intent = Intent.model_validate(state["intent"])
        missing = [name for name in ("consumer_id", "to_release") if not getattr(intent, name)]
        if not intent.initial and not intent.from_release:
            missing.append("from_release")
        if missing and self.model:
            response = await self.call(
                state, "extract", {"question": intent.question}, kind="model", cache=True
            )
            if not response["ok"]:
                return self.stop_for_error(response)
            extracted = Extracted.model_validate(response["data"])
            values = intent.model_dump()
            for name, value in extracted.model_dump().items():
                # The model cannot silently choose an unrelated current release or scope.
                if not values.get(name) and value and value in intent.question:
                    values[name] = value
            intent = Intent.model_validate(values)
        missing = [name for name in ("consumer_id", "to_release") if not getattr(intent, name)]
        if not intent.initial and not intent.from_release:
            missing.append("from_release (or explicit initial=true)")
        if missing:
            return {
                "phase": "NeedsInput",
                "intent": intent.model_dump(),
                "next_node": "end",
                "unresolved": ["Supply explicit " + ", ".join(missing)],
            }
        return {
            "phase": "Investigating",
            "intent": intent.model_dump(),
            "unresolved": [],
            "next_node": "facts",
        }

    async def facts(self, state):
        intent = state["intent"]
        releases = {}
        for side in ("from_release", "to_release"):
            if intent[side] is None:
                releases[side] = None
                continue
            response = await self.call(
                state,
                "get_release",
                {"dataset_id": intent["dataset_id"], "release_id": intent[side]},
                cache=True,
            )
            if not response["ok"]:
                return self.stop_for_error(response)
            releases[side] = response["data"]["pin"]
            if releases[side]["release_id"] != intent[side]:
                return self.stop_for_error(
                    failed("INTEGRITY_ERROR", "Release resolver changed identity")
                )
        book = await self.call(
            state,
            "search_runbook",
            {
                "query": "usage units " + intent["question"][:400],
                "source_version": intent["source_version"],
            },
            cache=True,
        )
        if not book["ok"]:
            return self.stop_for_error(book)
        if not book["data"].get("excerpts"):
            return self.stop_for_error(failed("NOT_FOUND", "No supporting runbook excerpts"))
        run = {}
        if intent["run_id"]:
            result = await self.call(state, "get_run", {"run_id": intent["run_id"]}, cache=True)
            if not result["ok"]:
                return self.stop_for_error(result)
            run = result["data"]
        return {
            "releases": releases,
            "runbook": book["data"],
            "run": run,
            "phase": "Investigating",
            "next_node": "compare",
            "unresolved": [],
        }

    async def compare(self, state):
        args = {
            name: state["intent"][name] for name in ("consumer_id", "from_release", "to_release")
        }
        response = await self.call(state, "compare_releases", args)
        if not response["ok"]:
            return self.stop_for_error(response)
        comparison = response["data"]
        request = comparison["request"]
        if (
            request["from_pin"] != state["releases"]["from_release"]
            or request["to_pin"] != state["releases"]["to_release"]
        ):
            return self.stop_for_error(
                failed("INTEGRITY_ERROR", "Comparison pins differ from the investigation")
            )
        if (
            state["intent"]["tenant_id"]
            and request["scope"]["tenant_id"] != state["intent"]["tenant_id"]
        ):
            return self.stop_for_error(
                failed("INVALID_REQUEST", "Consumer tenant differs from requested tenant")
            )
        if comparison["state"] == "FAILED":
            return self.stop_for_error({"error": comparison["error"]}, phase="Failed")
        complete = comparison["state"] == "COMPLETE" and comparison.get("result")
        return {
            "comparison": comparison,
            "phase": "Investigating",
            "next_node": "propose" if complete else "end",
            "unresolved": []
            if complete
            else ["Comparison is not complete; wait for its worker then resume"],
        }

    async def propose(self, state):
        cmp = state["comparison"]
        response = await self.call(
            state,
            "create_refresh_plan",
            {
                "comparison_id": cmp["comparison_id"],
                "consumer_id": state["intent"]["consumer_id"],
                "rebuild": state["intent"]["rebuild"],
            },
            cache=True,
        )
        if not response["ok"]:
            return self.stop_for_error(response)
        plan = response["data"]
        if (
            plan["source_pins"] != [cmp["request"]["from_pin"], cmp["request"]["to_pin"]]
            or plan["diff_hash"] != cmp["result"]["checksum"]
            or plan["consumer_id"] != state["intent"]["consumer_id"]
        ):
            return self.stop_for_error(
                failed("INTEGRITY_ERROR", "Plan does not bind the completed comparison")
            )
        facts = {
            **cmp["result"]["counts"],
            "target_release": state["intent"]["to_release"],
            "tenant_id": plan["registered_scope"]["tenant_id"],
            "affected_dates": [day["business_date"] for day in plan["affected_scope"]],
        }
        claims, unresolved = [], []
        if self.model:
            explanation = await self.call(
                state,
                "explain",
                {
                    "runbook": state["runbook"],
                    "tool_facts": facts,
                    "preview": cmp["result"]["preview"],
                },
                kind="model",
                cache=True,
            )
            if explanation["ok"]:
                claims = reconcile_claims(state["runbook"], explanation["data"], facts)
            else:
                unresolved.append(
                    "Model explanation unavailable; plan and factual summary come from tools"
                )
        evidence = {
            "facts": facts,
            "document_claims": claims,
            "conflicts": [claim for claim in claims if claim["status"] != "agrees"],
            "citations": {
                "releases": state["releases"],
                "comparison_id": cmp["comparison_id"],
                "diff_hash": plan["diff_hash"],
                "runbook_source": state["runbook"]["source_id"],
                "runbook_sha256": state["runbook"]["sha256"],
            },
            "preview_truncated": cmp["result"]["preview"]["truncated"],
            "summary": f"{facts['INSERT']} inserts, {facts['UPDATE']} updates, "
            f"{facts['DELETE']} deletes; "
            f"{plan['partition_count']} tenant-days to rebuild from {facts['target_release']}.",
            "interpretation": "configured_model" if self.model else "factual_only",
            "authority": "Pinned tool facts override document/model claims; "
            "no business write is complete.",
        }
        return {
            "plan": plan,
            "evidence": evidence,
            "phase": "WaitingApproval",
            "idempotency_key": "ops_" + fingerprint([self.binding, state["task_id"]])[:40],
            "next_node": "approval",
            "unresolved": unresolved,
        }

    async def approval(self, state):
        if datetime.fromisoformat(state["plan"]["expires_at"]) <= datetime.now(UTC):
            return {
                "phase": "Stopped",
                "next_node": "end",
                "unresolved": ["Plan expired; create a new investigation"],
            }
        decision = interrupt(
            {
                "kind": "independent_approval",
                "plan": state["plan"],
                "evidence": state["evidence"],
                "instructions": "Review via the human CLI/API; "
                "resume with its approval_id and plan_hash",
            }
        )
        if not isinstance(decision, dict) or set(decision) - {"approval_id", "plan_hash", "stop"}:
            return self.stop_for_error(
                failed("INVALID_REQUEST", "Invalid approval resume input"), "Stopped"
            )
        if decision.get("stop") is True:
            return {"phase": "Stopped", "next_node": "end", "unresolved": ["User stopped the task"]}
        if decision.get("plan_hash") != state["plan"]["plan_hash"]:
            return self.stop_for_error(
                failed("STALE_PLAN", "Reviewed plan hash changed"), "Stopped"
            )
        # An ID is a reference, not authority: the bridge checks the independent approval.
        args = ExecuteInput(
            plan_id=state["plan"]["plan_id"],
            approval_id=decision.get("approval_id"),
            idempotency_key=state["idempotency_key"],
        )
        return {"approval_id": args.approval_id, "phase": "Submitting", "next_node": "submit"}

    async def submit(self, state):
        result = await self.call(
            state,
            "execute_refresh",
            {
                "plan_id": state["plan"]["plan_id"],
                "approval_id": state["approval_id"],
                "idempotency_key": state["idempotency_key"],
            },
        )
        if not result["ok"]:
            code = result["error"]["code"]
            phase = "Reconciling" if code in ("SUBMISSION_UNKNOWN", "UNAVAILABLE") else "Stopped"
            return self.stop_for_error(result, phase)
        return self.job_result(result["data"])

    @staticmethod
    def job_result(job):
        status = job["state"]
        if status == "SUCCEEDED":
            if not job.get("output_consumer_release"):
                return Runtime.stop_for_error(
                    failed("INTEGRITY_ERROR", "Success lacks a consumer release"), "Failed"
                )
            phase = "Completed"
        elif status == "FAILED":
            phase = "Failed"
        else:
            phase = "Tracking"
        return {
            "job": job,
            "phase": phase,
            "next_node": "end",
            "unresolved": [str(job.get("error"))] if phase == "Failed" else [],
        }

    async def track(self, state):
        result = await self.call(state, "get_refresh_job", {"job_id": state["job"]["job_id"]})
        if not result["ok"]:
            return self.stop_for_error(result, "Tracking")
        return self.job_result(result["data"])

    @staticmethod
    def route(state):
        phase = state.get("phase", "New")
        if phase in ("New", "NeedsInput"):
            return "identify"
        if phase == "Investigating":
            return "compare" if state.get("releases") else "facts"
        return {
            "WaitingApproval": "approval",
            "Submitting": "submit",
            "Reconciling": "submit",
            "Tracking": "track",
        }.get(phase, "end")

    def graph(self, checkpointer):
        graph = StateGraph(State)
        nodes = ("identify", "facts", "compare", "propose", "approval", "submit", "track")
        for name in nodes:

            async def step(state, name=name):
                try:
                    return await getattr(self, name)(state)
                except BudgetExceeded as exc:
                    return {"phase": "Stopped", "next_node": "end", "unresolved": [str(exc)]}

            graph.add_node(name, step)
            graph.add_conditional_edges(
                name, lambda state: state["next_node"], {**{n: n for n in nodes}, "end": END}
            )
        graph.add_conditional_edges(START, self.route, {**{n: n for n in nodes}, "end": END})
        return graph.compile(checkpointer=checkpointer)

    async def drive(self, task_id, *, intent=None, resume=None, updates=None, budget=None):
        # Advisory host lock prevents competing resumes sharing a single task/checkpoint.
        lock_path = self.directory / (fingerprint(task_id) + ".lock")
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("Task is already executing; query its saved state") from exc
            async with AsyncSqliteSaver.from_conn_string(
                str(self.directory / "graph.sqlite")
            ) as saver:
                graph = self.graph(saver)
                config = {"configurable": {"thread_id": task_id}}
                checkpoint = await graph.aget_state(config)
                if intent is not None:
                    parsed = Intent.model_validate(intent).model_dump()
                    limits = budget or {
                        "model_calls": 6,
                        "tool_calls": 12,
                        "active_ms": 180000,
                        "call_timeout_ms": 15000,
                    }
                    if set(limits) != {
                        "model_calls",
                        "tool_calls",
                        "active_ms",
                        "call_timeout_ms",
                    } or any(type(v) is not int or v <= 0 for v in limits.values()):
                        raise ValueError("Budget limits must be positive integers")
                    self.ledger.create(
                        task_id, {"intent": parsed, "budget": limits, "binding": self.binding}
                    )
                    if checkpoint.values:
                        raise ValueError(
                            "Task already exists; resume or inspect its persisted state"
                        )
                    payload = {"task_id": task_id, "intent": parsed, "phase": "New"}
                else:
                    task = self.ledger.task(task_id)
                    if task["config"]["binding"] != self.binding:
                        raise ValueError("Tool service/identity changed for an existing task")
                    if updates and checkpoint.values.get("phase") != "NeedsInput":
                        raise ValueError(
                            "A proposed/approved task cannot change parameters; create a new task"
                        )
                    if checkpoint.values.get("phase") in ("Completed", "Failed", "Stopped"):
                        return await self.report(graph, config, task_id)
                    if not checkpoint.values:
                        # Recover a crash between task admission and the first graph checkpoint.
                        payload = {
                            "task_id": task_id,
                            "intent": task["config"]["intent"],
                            "phase": "New",
                        }
                    elif checkpoint.next:
                        if any(task.interrupts for task in checkpoint.tasks):
                            expires = checkpoint.values.get("plan", {}).get("expires_at")
                            if expires and datetime.fromisoformat(expires) <= datetime.now(UTC):
                                resume = {"stop": True}
                            if resume is None:
                                return await self.report(graph, config, task_id)
                            payload = Command(resume=resume)
                        else:
                            payload = None  # Resume pending work after a process/call interruption.
                    else:
                        payload = {"task_id": task_id}
                    if updates:
                        if checkpoint.values.get("phase") != "NeedsInput":
                            raise ValueError(
                                "A proposed/approved task cannot change parameters; "
                                "create a new task"
                            )
                        parsed = Intent.model_validate(
                            {**checkpoint.values["intent"], **updates}
                        ).model_dump()
                        payload = {
                            "task_id": task_id,
                            "intent": parsed,
                            "phase": "New",
                            "releases": {},
                            "comparison": {},
                            "unresolved": [],
                        }
                started = monotonic()
                active_before = self.ledger.task(task_id)["active_ms"]
                try:
                    await graph.ainvoke(payload, config, durability="sync")
                finally:
                    self.ledger.overhead(
                        task_id, int((monotonic() - started) * 1000), active_before
                    )
                return await self.report(graph, config, task_id)

    async def report(self, graph, config, task_id):
        snapshot = await graph.aget_state(config)
        budget = self.ledger.task(task_id)
        return {
            "state": dict(snapshot.values),
            "waiting": any(task.interrupts for task in snapshot.tasks),
            "budget": {key: budget[key] for key in ("model_calls", "tool_calls", "active_ms")},
            "limits": budget["config"]["budget"],
            "receipts": self.ledger.receipts(task_id),
        }

    async def status(self, task_id):
        async with AsyncSqliteSaver.from_conn_string(str(self.directory / "graph.sqlite")) as saver:
            return await self.report(
                self.graph(saver), {"configurable": {"thread_id": task_id}}, task_id
            )
