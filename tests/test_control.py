import pytest

from lakebridge.common import ConflictError, LakeBridgeError
from lakebridge.control import Control


@pytest.fixture
def control(tmp_path):
    store = Control(tmp_path)
    yield store
    store.close()


def candidate(run_id="run_one", passed=True):
    return {"run_id": run_id, "release_id": "rel_" + run_id, "validation": {"passed": passed}}


def test_unvalidated_release_is_never_visible(control):
    with control.writer("run_one"):
        with pytest.raises(LakeBridgeError, match="Unvalidated"):
            control.publish(candidate(passed=False), 0)
    assert control.status()["active_release"] is None


def test_persistent_ownership_prevents_stale_takeover(control):
    with control.writer("crashed_run"):
        pass  # Simulates process failure without release.
    with pytest.raises(ConflictError, match="Unfinished"):
        with control.writer("new_run"):
            pass
    with control.writer("crashed_run"):
        control.finish_writer("crashed_run")
    with control.writer("new_run"):
        control.finish_writer("new_run")


def test_second_connection_cannot_execute_same_run_concurrently(control, tmp_path):
    other = Control(tmp_path)
    try:
        with control.writer("same_run"):
            with pytest.raises(ConflictError, match="executing"):
                with other.writer("same_run"):
                    pytest.fail("parallel execution")
            control.finish_writer("same_run")
    finally:
        other.close()


def test_publish_generation_and_idempotent_recovery(control):
    with control.writer("run_one"):
        with pytest.raises(ConflictError, match="generation"):
            control.publish(candidate(), 7)
        control.publish(candidate(), 0)
        control.publish(candidate(), 0)
        control.finish_writer("run_one")
    assert len(control.releases()) == 1
    assert control.status()["generation"] == 1
    control.activate("rel_run_one", "rollback test", 1)
    with pytest.raises(ConflictError):
        control.activate("rel_run_one", "stale consumer", 1)
    assert control.status()["generation"] == 2


def test_writer_blocks_consumer_switch(control):
    with control.writer("run_one"):
        control.publish(candidate(), 0)
        with pytest.raises(ConflictError):
            control.activate("rel_run_one", "racing activation", 1)
        control.finish_writer("run_one")


def test_control_store_cannot_be_reused_for_another_dataset(control):
    control.bind("one", "/warehouse/one")
    control.bind("one", "/warehouse/one")
    with pytest.raises(LakeBridgeError, match="another namespace"):
        control.bind("two", "/warehouse/two")


def test_release_id_rejects_changed_payload_without_mutating_audit(control):
    release = {**candidate(), "tables": {"detail": 123}, "execution_seconds": 1}
    with control.writer("run_one"):
        control.publish(release, 0)
        # Retry timings are telemetry, not immutable release identity.
        control.publish({**release, "execution_seconds": 2, "queue_seconds": 0.1}, 0)
        with pytest.raises(ConflictError, match="different immutable"):
            control.publish({**release, "tables": {"detail": 456}}, 0)
        control.finish_writer("run_one")
    assert control.release()["tables"] == {"detail": 123}
    assert control.status()["generation"] == 1


def test_recovery_never_reactivates_a_rolled_back_release(control):
    for run, generation in (("one", 0), ("two", 1)):
        with control.writer(run):
            control.publish(candidate(run), generation)
            control.finish_writer(run)
    control.activate("rel_one", "consumer rollback", 2)
    control.publish(candidate("two"), 1)
    assert control.status()["active_release"] == "rel_one"
    assert control.status()["generation"] == 3
