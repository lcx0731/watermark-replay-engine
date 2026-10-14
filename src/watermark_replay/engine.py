"""All state transitions and outbox writes share one SQLite transaction."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
import sqlite3

INT64_MAX = 2**63 - 1


class ConflictError(ValueError):
    """An immutable offset or event ID was reused with different content."""


class OffsetError(ValueError):
    """A source offset skipped the next contiguous position."""


class StreamClosedError(ValueError):
    """A new record arrived after an explicit end-of-stream marker."""


def _integer(value, name, lower=0, upper=INT64_MAX):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"{name} must be an integer in [{lower}, {upper}]")
    return value


def _text(value, name):
    if not isinstance(value, str) or not value or len(value) > 200 or "\x00" in value:
        raise ValueError(f"{name} must be a nonempty string of at most 200 characters, without NUL")


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class Event:
    event_id: str
    key: str
    timestamp: int
    value: int

    def __post_init__(self):
        _text(self.event_id, "event_id")
        _text(self.key, "key")
        _integer(self.timestamp, "timestamp")
        _integer(self.value, "value", -INT64_MAX - 1)


class Engine:
    """One ordered source, integer seconds, half-open tumbling windows.

    The API acknowledges only after COMMIT. A stored receipt makes an exact
    offset retry a no-op even after finish. This is NOT an external delivery
    or distributed exactly-once guarantee. Use a dedicated local database.
    """

    def __init__(self, path, *, width=30, lag=10):
        self.width = _integer(width, "width", 1)
        self.lag = _integer(lag, "lag")
        self._db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self._db.row_factory = sqlite3.Row
        try:
            self._db.execute("PRAGMA journal_mode=DELETE")
            self._db.execute("PRAGMA synchronous=FULL")
            with self._transaction():
                statements = [
                    """CREATE TABLE IF NOT EXISTS metadata (
                        id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
                        width INTEGER NOT NULL, lag INTEGER NOT NULL, next_offset INTEGER NOT NULL,
                        max_time INTEGER, watermark INTEGER, finished INTEGER NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS receipts (
                        offset INTEGER PRIMARY KEY, payload TEXT NOT NULL, outcome TEXT NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS events (
                        event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL,
                        first_offset INTEGER NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS windows (
                        key TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL,
                        count INTEGER NOT NULL, total INTEGER NOT NULL CHECK(typeof(total)='integer'),
                        minimum INTEGER NOT NULL, maximum INTEGER NOT NULL, finalized INTEGER NOT NULL,
                        PRIMARY KEY(key,start))""",
                    "CREATE INDEX IF NOT EXISTS open_window_end ON windows(finalized,end)",
                    """CREATE TABLE IF NOT EXISTS outbox (
                        key TEXT NOT NULL, start INTEGER NOT NULL, emitted_offset INTEGER NOT NULL,
                        reason TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(key,start))""",
                ]
                for statement in statements:
                    self._db.execute(statement)
                config = self._db.execute("SELECT * FROM metadata WHERE id=1").fetchone()
                if config is None:
                    self._db.execute("INSERT INTO metadata VALUES (1,1,?,?,0,NULL,NULL,0)", (width, lag))
                elif (config["version"], config["width"], config["lag"]) != (1, width, lag):
                    raise ValueError("database schema version or configuration does not match")
        except BaseException:
            self._db.close()
            raise

    @contextmanager
    def _transaction(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def state(self):
        return dict(self._db.execute("SELECT * FROM metadata WHERE id=1").fetchone())

    @property
    def live_window_count(self):
        return self._db.execute("SELECT count(*) FROM windows WHERE finalized=0").fetchone()[0]

    def _checkpoint(self, stage):
        """No-op fault-injection seam; tests override it to fail before COMMIT."""

    def ingest(self, offset, event):
        if not isinstance(event, Event):
            raise ValueError("event must be an Event")
        end = (event.timestamp // self.width + 1) * self.width
        if end > INT64_MAX:
            raise ValueError("window end exceeds SQLite integer range")
        return self._apply(offset, {"kind": "event", "event": asdict(event)}, event)

    def finish(self, offset):
        """Explicit end of the finite source, not a wall-clock timeout."""
        return self._apply(offset, {"kind": "finish"}, None)

    def _apply(self, offset, record, event):
        _integer(offset, "offset", upper=INT64_MAX - 1)
        payload = _json(record)
        with self._transaction():
            previous = self._db.execute("SELECT * FROM receipts WHERE offset=?", (offset,)).fetchone()
            if previous is not None:
                if previous["payload"] != payload:
                    raise ConflictError("source offset payload changed")
                return json.loads(previous["outcome"])
            state = self.state
            if offset != state["next_offset"]:
                raise OffsetError(f"expected offset {state['next_offset']}, received {offset}")
            if state["finished"]:
                raise StreamClosedError("source has finished")
            status = "finished"
            if event is not None:
                status = self._event(offset, event, state)
            else:
                self._db.execute("UPDATE metadata SET finished=1 WHERE id=1")
            self._checkpoint("after_event")
            emitted = self._finalize(offset, finish=event is None)
            self._checkpoint("after_finalize")
            self._db.execute("UPDATE metadata SET next_offset=? WHERE id=1", (offset + 1,))
            outcome = {"offset": offset, "status": status, "emitted": emitted,
                       "watermark": self.state["watermark"]}
            self._db.execute("INSERT INTO receipts VALUES (?,?,?)", (offset, payload, _json(outcome)))
            self._checkpoint("before_commit")
        return outcome

    def _event(self, offset, event, state):
        payload = _json(asdict(event))
        existing = self._db.execute("SELECT payload FROM events WHERE event_id=?", (event.event_id,)).fetchone()
        if existing is not None:
            if existing["payload"] != payload:
                raise ConflictError("event ID payload changed")
            return "duplicate"
        start = event.timestamp // self.width * self.width
        end = start + self.width
        late = state["watermark"] is not None and end <= state["watermark"]
        status = "late" if late else "accepted"
        self._db.execute("INSERT INTO events VALUES (?,?,?,?)", (event.event_id, payload, status, offset))
        if not late:
            old = self._db.execute("SELECT * FROM windows WHERE key=? AND start=?", (event.key, start)).fetchone()
            count = 1 if old is None else old["count"] + 1
            total = event.value if old is None else old["total"] + event.value
            _integer(count, "aggregate count", 1)
            _integer(total, "aggregate total", -INT64_MAX - 1)
            minimum = event.value if old is None else min(old["minimum"], event.value)
            maximum = event.value if old is None else max(old["maximum"], event.value)
            if old is not None and old["finalized"]:
                raise AssertionError("cannot mutate a finalized window")
            self._db.execute("""INSERT INTO windows VALUES (?,?,?,?,?,?,?,0)
                ON CONFLICT(key,start) DO UPDATE SET count=excluded.count, total=excluded.total,
                minimum=excluded.minimum, maximum=excluded.maximum""",
                (event.key, start, end, count, total, minimum, maximum))
        max_time = event.timestamp if state["max_time"] is None else max(state["max_time"], event.timestamp)
        self._db.execute("UPDATE metadata SET max_time=?,watermark=? WHERE id=1", (max_time, max_time - self.lag))
        return status

    def _finalize(self, offset, *, finish):
        watermark = self.state["watermark"]
        query = "SELECT * FROM windows WHERE finalized=0"
        rows = self._db.execute(query + ("" if finish else " AND end<=? ") + " ORDER BY end,key,start",
                                () if finish else (watermark,)).fetchall()
        for row in rows:
            data = {k: row[k] for k in ["key", "start", "end", "count", "total", "minimum", "maximum"]}
            self._db.execute("INSERT INTO outbox VALUES (?,?,?,?,?)",
                             (row["key"], row["start"], offset, "finish" if finish else "watermark", _json(data)))
            self._db.execute("UPDATE windows SET finalized=1 WHERE key=? AND start=?", (row["key"], row["start"]))
        return len(rows)

    def snapshot(self):
        """Stable logical snapshot; excludes file layout, wall clocks, and paths.

        Use with no concurrent writer; this helper issues multiple SELECTs.
        """
        return {"metadata": self.state, **{
            table: [dict(row) for row in self._db.execute(f"SELECT * FROM {table} ORDER BY {order}")]
            for table, order in [("receipts", "offset"), ("events", "event_id"),
                                 ("windows", "key,start"), ("outbox", "key,start")]
        }}

    def outputs(self):
        return [dict(row) | {"payload": json.loads(row["payload"])}
                for row in self._db.execute("SELECT * FROM outbox ORDER BY key,start")]
