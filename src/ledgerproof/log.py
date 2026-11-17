"""An append-only event log — the minimal faithful stand-in for a topic.

What it keeps from real brokers: immutable ordered records, offsets,
read-from-offset consumption, and at-least-once delivery (a consumer that
crashes before committing will see the same records again). What it adds
for auditability: a hash chain, so the log is tamper-evident — any
mutation of history breaks verification from that point forward.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterator


@dataclass(frozen=True)
class Record:
    offset: int
    event: dict[str, Any]
    prev_hash: str
    hash: str


def _hash(offset: int, event: dict, prev_hash: str) -> str:
    payload = json.dumps({"o": offset, "e": event, "p": prev_hash}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


GENESIS = "0" * 64


class EventLog:
    """Append-only. There is deliberately no update or delete — corrections
    in an event-sourced system are new, compensating events, never edits.
    History is evidence; evidence is not rewritten."""

    def __init__(self) -> None:
        self._records: list[Record] = []

    def append(self, event: dict[str, Any]) -> Record:
        offset = len(self._records)
        prev = self._records[-1].hash if self._records else GENESIS
        rec = Record(offset, event, prev, _hash(offset, event, prev))
        self._records.append(rec)
        return rec

    def read_from(self, offset: int) -> Iterator[Record]:
        """At-least-once by construction: callers track their own committed
        offset; re-reading after a crash replays everything uncommitted."""
        yield from self._records[offset:]

    def __len__(self) -> int:
        return len(self._records)

    def verify_chain(self) -> int:
        """Return the count of valid records from the head; a full pass
        returns len(self). Any tampering surfaces as a shortened count."""
        prev = GENESIS
        for i, rec in enumerate(self._records):
            if rec.prev_hash != prev or rec.hash != _hash(rec.offset, rec.event, prev):
                return i
            prev = rec.hash
        return len(self._records)
