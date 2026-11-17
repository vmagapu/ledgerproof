"""The consumer: where exactly-once is actually earned.

The broker gives you at-least-once. Exactly-once PROCESSING is built from
two parts working together:

  1. idempotent application        (the ledger skips duplicate tx_ids)
  2. atomic state+offset commit    (balances and progress persist together)

Commit the offset separately from the state and a crash between the two
either loses events or double-applies them. Commit them together and a
crash anywhere merely replays work that deduplication absorbs. This module
makes that argument executable: `CrashPoint` lets tests kill the consumer
at the worst possible moments and prove the end state is identical.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum

from .ledger import Ledger
from .log import EventLog


class CrashPoint(Enum):
    NONE = "none"
    AFTER_APPLY_BEFORE_COMMIT = "after_apply_before_commit"  # the classic double-apply trap


class SimulatedCrash(Exception):
    pass


@dataclass
class StateStore:
    """Stands in for the transactional store (a DB row, a changelog topic).
    Commit is atomic: the durable ledger replaces wholesale, balances and
    committed_offset as one unit."""

    durable: Ledger

    def load(self) -> Ledger:
        return copy.deepcopy(self.durable)  # crash loses anything uncommitted

    def commit(self, working: Ledger) -> None:
        self.durable = copy.deepcopy(working)


class Processor:
    def __init__(self, log: EventLog, store: StateStore):
        self.log = log
        self.store = store

    def run(self, crash: CrashPoint = CrashPoint.NONE, crash_at_offset: int | None = None) -> Ledger:
        """Consume from the durable committed offset to the end of the log,
        committing state+offset together after each record. Raises
        SimulatedCrash at the configured point; a subsequent run() recovers
        and must converge to the same state as a crash-free run."""
        working = self.store.load()
        for record in self.log.read_from(working.committed_offset):
            working.apply(record)

            if (
                crash is CrashPoint.AFTER_APPLY_BEFORE_COMMIT
                and record.offset == crash_at_offset
            ):
                # State mutated in memory, offset not committed: on recovery
                # this record WILL be redelivered. Idempotency must absorb it.
                raise SimulatedCrash(f"crashed after applying offset {record.offset}")

            working.committed_offset = record.offset + 1
            self.store.commit(working)  # ONE atomic step: balances + offset
        return self.store.durable


def replay(log: EventLog) -> Ledger:
    """Rebuild the projection from offset 0 — the event-sourcing dividend.
    Any bug's blast radius is bounded by 'fix the code, replay the log';
    the log, not the projection, is the system of record."""
    ledger = Ledger()
    for record in log.read_from(0):
        ledger.apply(record)
        ledger.committed_offset = record.offset + 1
    return ledger
