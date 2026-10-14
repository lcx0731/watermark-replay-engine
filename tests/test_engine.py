from dataclasses import replace
from pathlib import Path
import random
import subprocess
import sys

import pytest

from watermark_replay import ConflictError, Engine, Event, OffsetError, StreamClosedError
from watermark_replay.engine import INT64_MAX
from watermark_replay.experiment import batch_oracle, run_case, workload


def e(i, timestamp, value=1, key="a"):
    return Event(str(i), key, timestamp, value)


def test_half_open_windows_and_key_isolation(tmp_path):
    with Engine(tmp_path / "state.sqlite", width=10, lag=100) as engine:
        events = [e(0, 0, 2), e(1, 9, -3), e(2, 10, 4), e(3, 9, 7, "b")]
        for i, event in enumerate(events):
            engine.ingest(i, event)
        engine.finish(4)
        rows = [r["payload"] for r in engine.outputs()]
        assert rows == [
            {"key": "a", "start": 0, "end": 10, "count": 2, "total": -1, "minimum": -3, "maximum": 2},
            {"key": "a", "start": 10, "end": 20, "count": 1, "total": 4, "minimum": 4, "maximum": 4},
            {"key": "b", "start": 0, "end": 10, "count": 1, "total": 7, "minimum": 7, "maximum": 7},
        ]


@pytest.mark.parametrize("frontier,status", [(9, "accepted"), (10, "late"), (11, "late")])
def test_window_end_watermark_boundary(tmp_path, frontier, status):
    with Engine(tmp_path / "state.sqlite", width=10, lag=0) as engine:
        engine.ingest(0, e(0, frontier))
        assert engine.ingest(1, e(1, 3))["status"] == status


def test_older_than_watermark_but_in_open_window_is_accepted(tmp_path):
    with Engine(tmp_path / "state.sqlite", width=10, lag=0) as engine:
        engine.ingest(0, e(0, 15))
        assert engine.ingest(1, e(1, 11))["status"] == "accepted"
        assert engine.state["watermark"] == 15


def test_exact_offset_replay_after_finish_is_noop(tmp_path):
    with Engine(tmp_path / "state.sqlite") as engine:
        original = engine.ingest(0, e(0, 0))
        finished = engine.finish(1)
        before = engine.snapshot()
        assert engine.ingest(0, e(0, 0)) == original
        assert engine.finish(1) == finished
        assert engine.snapshot() == before


def test_duplicate_id_at_new_offset_commits_receipt_not_aggregate(tmp_path):
    with Engine(tmp_path / "state.sqlite") as engine:
        engine.ingest(0, e(0, 10, 5))
        assert engine.ingest(1, e(0, 10, 5))["status"] == "duplicate"
        state = engine.snapshot()
        assert len(state["receipts"]) == 2 and len(state["events"]) == 1
        assert state["windows"][0]["count"] == 1 and state["windows"][0]["total"] == 5


def test_late_event_is_audited_and_cannot_be_revived_by_redelivery(tmp_path):
    with Engine(tmp_path / "state.sqlite", width=10, lag=0) as engine:
        engine.ingest(0, e(0, 100))
        assert engine.ingest(1, e(1, 0))["status"] == "late"
        assert engine.ingest(2, e(1, 0))["status"] == "duplicate"
        snapshot = engine.snapshot()
        assert len(snapshot["windows"]) == 1
        assert [r["status"] for r in snapshot["events"]] == ["accepted", "late"]


@pytest.mark.parametrize("same_offset", [False, True])
@pytest.mark.parametrize("field,value", [("value", 2), ("timestamp", 99), ("key", "changed")])
def test_conflict_rolls_back_without_consuming_offset(tmp_path, same_offset, field, value):
    with Engine(tmp_path / "state.sqlite") as engine:
        event = e(0, 0)
        engine.ingest(0, event)
        before = engine.snapshot()
        with pytest.raises(ConflictError):
            engine.ingest(0 if same_offset else 1, replace(event, **{field: value}))
        assert engine.snapshot() == before
        engine.ingest(1, e(1, 1))


def test_gap_and_new_records_after_finish_fail_without_mutation(tmp_path):
    with Engine(tmp_path / "state.sqlite") as engine:
        initial = engine.snapshot()
        with pytest.raises(OffsetError):
            engine.ingest(1, e(0, 0))
        assert engine.snapshot() == initial
        engine.finish(0)
        before = engine.snapshot()
        with pytest.raises(StreamClosedError):
            engine.ingest(1, e(1, 1))
        with pytest.raises(StreamClosedError):
            engine.finish(1)
        with pytest.raises(ConflictError):
            engine.ingest(0, e(0, 0))
        assert engine.snapshot() == before


def test_empty_stream_finish(tmp_path):
    with Engine(tmp_path / "state.sqlite") as engine:
        assert engine.finish(0)["emitted"] == 0
        assert engine.outputs() == []
        assert engine.state["finished"] == 1


def test_final_output_is_immutable_and_only_emitted_once(tmp_path):
    with Engine(tmp_path / "state.sqlite", width=10, lag=0) as engine:
        engine.ingest(0, e(0, 0, 3))
        assert engine.ingest(1, e(1, 10, 4))["emitted"] == 1
        original = engine.outputs()[0]
        assert engine.ingest(2, e(2, 9, 100))["status"] == "late"
        engine.finish(3)
        assert engine.outputs()[0] == original
        assert len(engine.outputs()) == 2


@pytest.mark.parametrize("stage", ["after_event", "after_finalize", "before_commit"])
def test_exception_rolls_back_all_tables(tmp_path, stage):
    class FaultEngine(Engine):
        fail = False

        def _checkpoint(self, current):
            if self.fail and current == stage:
                raise RuntimeError("injected failure")

    with FaultEngine(tmp_path / "state.sqlite", width=10, lag=0) as engine:
        engine.ingest(0, e(0, 0))
        before = engine.snapshot()
        engine.fail = True
        with pytest.raises(RuntimeError, match="injected failure"):
            engine.ingest(1, e(1, 20))
        assert engine.snapshot() == before
        engine.fail = False
        engine.ingest(1, e(1, 20))
        assert engine.state["next_offset"] == 2


@pytest.mark.parametrize("stage", ["after_event", "after_finalize", "before_commit", "after_commit"])
def test_actual_process_exit_then_recovery(tmp_path, stage):
    path = tmp_path / "state.sqlite"
    with Engine(path, width=10, lag=0) as engine:
        engine.ingest(0, Event("first", "a", 0, 3))
        before = engine.snapshot()
    worker = Path(__file__).with_name("crash_worker.py")
    completed = subprocess.run([sys.executable, str(worker), str(path), stage],
                               capture_output=True, text=True, timeout=20)
    assert completed.returncode == (74 if stage == "after_commit" else 73), completed.stderr
    with Engine(path, width=10, lag=0) as engine:
        assert engine._db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        if stage != "after_commit":
            assert engine.snapshot() == before
        else:
            assert engine.state["next_offset"] == 2
        engine.ingest(1, Event("second", "a", 20, 5))
        engine.finish(2)
        recovered = engine.snapshot()
    with Engine(tmp_path / "clean.sqlite", width=10, lag=0) as clean:
        clean.ingest(0, Event("first", "a", 0, 3))
        clean.ingest(1, Event("second", "a", 20, 5))
        clean.finish(2)
        assert clean.snapshot() == recovered


@pytest.mark.parametrize("seed", range(6))
def test_random_order_and_redeliveries_match_independent_oracle(tmp_path, seed):
    rng = random.Random(seed)
    events = [e(i, rng.randrange(0, 120), rng.randrange(-10, 20), str(i % 3)) for i in range(50)]
    deliveries = list(enumerate(events + events[::5]))
    rng.shuffle(deliveries)
    lag = seed * 8
    statuses, outputs = batch_oracle(deliveries, width=10, lag=lag)
    with Engine(tmp_path / "state.sqlite", width=10, lag=lag) as engine:
        actual, watermarks = [], []
        for offset, (_, event) in enumerate(deliveries):
            actual.append(engine.ingest(offset, event)["status"])
            watermarks.append(engine.state["watermark"])
        engine.finish(len(deliveries))
        assert actual == statuses
        assert [row["payload"] for row in engine.outputs()] == outputs
        assert watermarks == sorted(watermarks)


def test_sufficient_lag_makes_aggregate_order_invariant(tmp_path):
    events = [e(i, i, i - 15, str(i % 2)) for i in range(30)]
    outputs = []
    for seed in range(3):
        random.Random(seed).shuffle(events)
        with Engine(tmp_path / f"state-{seed}.sqlite", width=10, lag=100) as engine:
            for offset, event in enumerate(events):
                assert engine.ingest(offset, event)["status"] == "accepted"
            engine.finish(len(events))
            outputs.append([row["payload"] for row in engine.outputs()])
    assert outputs[0] == outputs[1] == outputs[2]


def test_order_can_change_late_admission(tmp_path):
    early, late = e(0, 0), e(1, 20)
    a, _ = batch_oracle([(0, early), (1, late)], width=10, lag=0)
    b, _ = batch_oracle([(0, late), (1, early)], width=10, lag=0)
    assert a == ["accepted", "accepted"]
    assert b == ["accepted", "late"]


@pytest.mark.parametrize("value,extra", [(INT64_MAX, 1), (-INT64_MAX - 1, -1)])
def test_integer_sum_overflow_fails_without_promoting_to_float(tmp_path, value, extra):
    with Engine(tmp_path / "state.sqlite") as engine:
        engine.ingest(0, e(0, 0, value))
        before = engine.snapshot()
        with pytest.raises(ValueError, match="aggregate total"):
            engine.ingest(1, e(1, 0, extra))
        assert engine.snapshot() == before


def test_configuration_mismatch_rejected_on_reopen(tmp_path):
    path = tmp_path / "state.sqlite"
    with Engine(path, width=10, lag=3) as engine:
        engine.ingest(0, e(0, 0))
        before = engine.snapshot()
    with pytest.raises(ValueError, match="configuration"):
        Engine(path, width=10, lag=4)
    with Engine(path, width=10, lag=3) as engine:
        assert engine.snapshot() == before


@pytest.mark.parametrize("kwargs", [{"width": 0}, {"width": True}, {"lag": -1}, {"lag": 0.5}])
def test_invalid_configuration(tmp_path, kwargs):
    with pytest.raises(ValueError):
        Engine(tmp_path / "state.sqlite", **kwargs)


@pytest.mark.parametrize("field,value", [("event_id", ""), ("key", "\x00"),
                                         ("key", "x" * 201), ("timestamp", -1),
                                         ("timestamp", True), ("value", 1.1),
                                         ("value", float("nan")), ("value", INT64_MAX + 1)])
def test_invalid_events(field, value):
    with pytest.raises(ValueError):
        replace(e(0, 0), **{field: value})


@pytest.mark.parametrize("offset", [-1, 0.1, True, INT64_MAX])
def test_invalid_offsets(tmp_path, offset):
    with Engine(tmp_path / "state.sqlite") as engine:
        with pytest.raises(ValueError):
            engine.ingest(offset, e(0, 0))
        assert engine.state["next_offset"] == 0


def test_unrepresentable_window_end_rejected(tmp_path):
    with Engine(tmp_path / "state.sqlite") as engine:
        with pytest.raises(ValueError, match="window end"):
            engine.ingest(0, e(0, INT64_MAX))
        assert engine.state["next_offset"] == 0


def test_synthetic_workload_is_reproducible_and_stress_is_explicit():
    original = workload()
    assert original == workload()
    assert len(original) == 660 and len({ev.event_id for _, ev in original}) == 600
    assert len(workload(clock_skew=True)) == 661
    assert original == sorted(original, key=lambda row: (row[0], row[1].event_id))


@pytest.mark.parametrize("stage", ["after_event", "after_finalize", "before_commit"])
def test_failed_finish_does_not_close_stream_or_publish(tmp_path, stage):
    class FaultEngine(Engine):
        fail = False

        def _checkpoint(self, current):
            if self.fail and current == stage:
                raise RuntimeError("finish interrupted")

    with FaultEngine(tmp_path / "state.sqlite") as engine:
        engine.ingest(0, e(0, 0))
        before = engine.snapshot()
        engine.fail = True
        with pytest.raises(RuntimeError, match="finish interrupted"):
            engine.finish(1)
        assert engine.snapshot() == before
        engine.fail = False
        engine.ingest(1, e(1, 1))
        engine.finish(2)
        assert engine.outputs()[0]["payload"]["count"] == 2


def test_small_end_to_end_demo_and_clock_metric_omission(tmp_path):
    deliveries = [(0, e(0, 0)), (1, e(0, 0)), (31, e(1, 30))]
    summary, trace, outputs = run_case(deliveries, lag=0, directory=tmp_path,
                                       label="small", measure_delay=False)
    assert summary["accepted"] == 2 and summary["duplicate_deliveries"] == 1
    assert summary["oracle_matches"] and summary["restart_and_replay_matches"]
    assert summary["median_finalization_arrival_minus_window_end_seconds"] is None
    assert len(trace) == 3 and len(outputs) == 2
