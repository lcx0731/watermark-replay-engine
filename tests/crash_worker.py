"""Run only from the subprocess crash tests; terminate before or after COMMIT."""

import os
import sys

from watermark_replay import Engine, Event


class CrashEngine(Engine):
    def _checkpoint(self, stage):
        if stage == sys.argv[2]:
            os._exit(73)


with CrashEngine(sys.argv[1], width=10, lag=0) as engine:
    engine.ingest(1, Event("second", "a", 20, 5))
    # Commit happened but an upstream caller never receives the acknowledgement.
    if sys.argv[2] == "after_commit":
        os._exit(74)
