import asyncio

import httpx
from mcp import Client

from lakebridge_ops.tools import BusinessTools, create_server


def test_official_sdk_fixed_catalog_types_and_annotations():
    async def scenario():
        async def api(request):
            return httpx.Response(200, json={"pin": {"release_id": "r1"}})

        tools = BusinessTools("http://bridge", "agent", "saas", transport=httpx.MockTransport(api))
        async with Client(create_server(tools)) as client:
            catalog = {tool.name: tool for tool in (await client.list_tools()).tools}
            assert set(catalog) == {
                "get_release",
                "get_run",
                "search_runbook",
                "compare_releases",
                "create_refresh_plan",
                "execute_refresh",
                "get_refresh_job",
            }
            assert catalog["get_release"].annotations.read_only_hint
            assert not catalog["create_refresh_plan"].annotations.read_only_hint
            result = await client.call_tool(
                "get_release", {"request": {"dataset_id": "saas", "release_id": "r1"}}
            )
            assert result.structured_content["data"]["pin"]["release_id"] == "r1"
            invalid = await client.call_tool(
                "get_release", {"request": {"dataset_id": "saas", "release_id": "../../etc/passwd"}}
            )
            assert invalid.is_error
            forbidden = await client.call_tool(
                "get_release", {"request": {"dataset_id": "other", "release_id": "r1"}}
            )
            assert forbidden.structured_content["error"]["code"] == "FORBIDDEN"
        await tools.close()

    asyncio.run(scenario())


def test_lost_submission_response_reconciles_without_new_job():
    async def scenario():
        accepted = None
        submissions = []

        async def api(request):
            nonlocal accepted
            if request.method == "GET":
                return httpx.Response(200, json={"job": accepted})
            submissions.append(request.content)
            accepted = {
                "job_id": "job_original",
                "plan_id": "p1",
                "approval_id": "a1",
                "state": "RUNNING",
            }
            raise httpx.ReadError("connection lost after commit")

        tools = BusinessTools("http://bridge", "agent", "saas", transport=httpx.MockTransport(api))
        args = {"plan_id": "p1", "approval_id": "a1", "idempotency_key": "stable-task"}
        first = await tools.call("execute_refresh", args)
        second = await tools.call("execute_refresh", args)
        assert first == second
        assert first["data"]["job_id"] == "job_original"
        assert len(submissions) == 1
        assert not (await tools.call("approve", {"approval": True}))["ok"]
        assert not (await tools.call("execute_refresh", {**args, "approval": True}))["ok"]
        await tools.close()

    asyncio.run(scenario())


def test_proxy_error_after_commit_is_an_unknown_outcome_then_reconciled():
    async def scenario():
        job = None
        posts = 0

        async def api(request):
            nonlocal job, posts
            if request.method == "GET":
                return httpx.Response(200, json={"job": job})
            posts += 1
            job = {"job_id": "accepted", "plan_id": "p", "approval_id": "a", "state": "RUNNING"}
            return httpx.Response(502, json={"detail": "proxy lost upstream response"})

        tools = BusinessTools("http://bridge", "agent", "saas", transport=httpx.MockTransport(api))
        result = await tools.call(
            "execute_refresh", {"plan_id": "p", "approval_id": "a", "idempotency_key": "same"}
        )
        assert result["ok"] and result["data"]["job_id"] == "accepted"
        assert posts == 1
        await tools.close()

    asyncio.run(scenario())
