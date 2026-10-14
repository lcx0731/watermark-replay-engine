"""A seeded workload, an independent batch oracle, and restart/replay checks."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import random
import statistics
import tempfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .engine import Engine, Event


def workload(*, clock_skew=False, seed=20261014):
    """600 unique events, 60 redeliveries; arrival order is a durable source log."""
    rng = random.Random(seed)
    deliveries = []
    for i in range(600):
        timestamp = i // 2
        value = rng.randrange(1000, 10000)
        delay = rng.randrange(20, 61) if i % 20 == 0 else rng.randrange(0, 13)
        event = Event(f"e{i:04d}", f"sensor-{i % 5}", timestamp, value)
        arrival = timestamp + delay
        deliveries.append((arrival, event))
        if i % 10 == 0:
            deliveries.append((arrival + rng.randrange(1, 21), event))
    if clock_skew:
        # A bad producer clock arrives early with a far-future event timestamp.
        deliveries.append((60, Event("clock-skew", "sensor-0", 3600, 5000)))
    return sorted(deliveries, key=lambda item: (item[0], item[1].event_id))


def batch_oracle(deliveries, *, width, lag):
    """Classify with a plain-Python frontier, then batch-aggregate accepted rows.

    This has no SQLite state, incremental aggregates, receipts, or outbox logic.
    Arrival order is intentional: changing it can change late admission.
    """
    seen, accepted, statuses = {}, [], []
    frontier = None
    for _, event in deliveries:
        if event.event_id in seen:
            if seen[event.event_id] != event:
                raise ValueError("conflicting event in oracle input")
            statuses.append("duplicate")
            continue
        seen[event.event_id] = event
        window_end = (event.timestamp // width + 1) * width
        late = frontier is not None and window_end <= frontier - lag
        statuses.append("late" if late else "accepted")
        if not late:
            accepted.append(event)
        frontier = event.timestamp if frontier is None else max(frontier, event.timestamp)
    groups = defaultdict(list)
    for event in accepted:
        groups[(event.key, event.timestamp // width * width)].append(event.value)
    outputs = [{"key": key, "start": start, "end": start + width,
                "count": len(values), "total": sum(values),
                "minimum": min(values), "maximum": max(values)}
               for (key, start), values in sorted(groups.items())]
    return statuses, outputs


def _digest(snapshot):
    text = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest()


def run_case(deliveries, *, lag, directory, label, measure_delay=True):
    width = 30
    reference_statuses, reference_outputs = batch_oracle(deliveries, width=width, lag=lag)
    with Engine(directory / f"{label}-clean.sqlite", width=width, lag=lag) as engine:
        trace, observed_statuses = [], []
        for offset, (arrival, event) in enumerate(deliveries):
            outcome = engine.ingest(offset, event)
            observed_statuses.append(outcome["status"])
            live = engine.live_window_count
            trace.append({"offset": offset, "arrival": arrival, "event_time": event.timestamp,
                          "status": outcome["status"], "watermark": outcome["watermark"],
                          "live_windows": live})
        engine.finish(len(deliveries))
        outputs, snapshot = engine.outputs(), engine.snapshot()
        if observed_statuses != reference_statuses:
            raise AssertionError("online admission disagrees with independent oracle")
        if [row["payload"] for row in outputs] != reference_outputs:
            raise AssertionError("online aggregates disagree with batch oracle")
    # Reopen at a deterministic interruption boundary; replay an acknowledged
    # suffix, then all committed offsets after finish to mimic lost ACKs.
    path = directory / f"{label}-restarted.sqlite"
    midpoint = len(deliveries) // 2
    with Engine(path, width=width, lag=lag) as engine:
        for offset in range(midpoint):
            engine.ingest(offset, deliveries[offset][1])
    with Engine(path, width=width, lag=lag) as engine:
        for offset in range(max(0, midpoint - 7), len(deliveries)):
            engine.ingest(offset, deliveries[offset][1])
        engine.finish(len(deliveries))
        for offset, (_, event) in enumerate(deliveries):
            engine.ingest(offset, event)
        engine.finish(len(deliveries))
        if engine.snapshot() != snapshot:
            raise AssertionError("restart and replay changed logical state")
    counts = Counter(observed_statuses)
    final_arrival = max(arrival for arrival, _ in deliveries) + 1
    emission_delays = [
        (final_arrival if row["emitted_offset"] == len(deliveries)
         else deliveries[row["emitted_offset"]][0]) - row["payload"]["end"]
        for row in outputs
    ]
    summary = {"label": label, "lag_seconds": lag, "deliveries": len(deliveries),
               "unique_events": len(snapshot["events"]), "accepted": counts["accepted"],
               "late": counts["late"], "duplicate_deliveries": counts["duplicate"],
               "final_windows": len(outputs), "peak_live_windows": max(row["live_windows"] for row in trace),
               "watermark_finalizations": sum(row["reason"] == "watermark" for row in outputs),
               "finish_finalizations": sum(row["reason"] == "finish" for row in outputs),
               "median_finalization_arrival_minus_window_end_seconds":
                   statistics.median(emission_delays) if measure_delay else None,
               "accepted_value_total": sum(row["payload"]["total"] for row in outputs),
               "oracle_matches": True, "restart_and_replay_matches": True,
               "logical_state_sha256": _digest(snapshot)}
    return summary, trace, outputs


def run_experiment():
    summaries, traces, outputs = [], {}, {}
    with tempfile.TemporaryDirectory(prefix="watermark-demo-") as task_directory:
        directory = Path(task_directory)
        for lag in [0, 10, 30]:
            label = f"lag-{lag}"
            summary, trace, rows = run_case(workload(), lag=lag, directory=directory, label=label)
            summaries.append(summary)
            traces[label], outputs[label] = trace, rows
        summary, trace, rows = run_case(workload(clock_skew=True), lag=30,
                                       directory=directory, label="clock-skew-lag-30", measure_delay=False)
        summaries.append(summary)
        traces[summary["label"]], outputs[summary["label"]] = trace, rows
    return {"seed": 20261014, "window_width_seconds": 30,
            "data": "deterministic synthetic integer telemetry", "scenarios": summaries}, traces, outputs


def plots(report, traces, output):
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    normal = report["scenarios"][:3]
    labels = [str(row["lag_seconds"]) for row in normal]
    for ax, metric, title, ylabel, color in zip(
        axes, ["late", "median_finalization_arrival_minus_window_end_seconds", "peak_live_windows"],
        ["Unique events rejected", "Median finalization delay", "Peak mutable windows"],
        ["Event count (out of 600)", "Arrival time minus window end (s)", "Window count"],
        ["#BD613C", "#007F86", "#536993"],
    ):
        heights = [row[metric] for row in normal]
        bars = ax.bar(labels, heights, color=color)
        ax.bar_label(bars, padding=3)
        ax.set(xlabel="Watermark lag (s)", ylabel=ylabel, title=title)
        ax.set_ylim(0, max(heights) * 1.25 + 1)
    fig.suptitle("More waiting: fewer late drops, later final output, more live windows", fontsize=13)
    fig.savefig(output / "lateness-tradeoff.png", dpi=160)
    plt.close(fig)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), constrained_layout=True)
    for ax, label, title in zip(axes, ["lag-30", "clock-skew-lag-30"], ["Normal clocks", "One timestamp jumps to 3600 s"]):
        trace = traces[label]
        for status, color in [("accepted", "#007F86"), ("late", "#BD613C")]:
            subset = [row for row in trace if row["status"] == status]
            ax.scatter([row["arrival"] for row in subset], [row["event_time"] for row in subset],
                       s=9, alpha=0.6, color=color, label=status.capitalize())
        ax.step([row["arrival"] for row in trace], [row["watermark"] for row in trace],
                where="post", color="#272F3D", label="Watermark", linewidth=1.3)
        ax.set(title=title, xlabel="Arrival time (s)", ylabel="Event time / watermark (s)")
        ax.legend(loc="upper left" if label == "lag-30" else "center right")
        ax.grid(alpha=0.15)
    fig.suptitle("A watermark is a clock-dependent policy, not proof of completeness", fontsize=13)
    fig.savefig(output / "clock-skew-stress.png", dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("results"))
    args = parser.parse_args()
    report, traces, outputs = run_experiment()
    args.output.mkdir(parents=True, exist_ok=True)
    for name, data in [("summary.json", report), ("window-outputs.json", outputs)]:
        (args.output / name).write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    plots(report, traces, args.output)
    for row in report["scenarios"]:
        print(f"{row['label']}: accepted={row['accepted']}, late={row['late']}, "
              f"duplicates={row['duplicate_deliveries']}; oracle and replay match")


if __name__ == "__main__":
    main()
