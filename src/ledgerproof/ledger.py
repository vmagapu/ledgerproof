"""A double-entry ledger as a projection over the event log.

Two invariants do all the work:

1. Double entry — every transaction's postings sum to exactly zero, so the
   books balance by construction, not by reconciliation heroics.
2. Idempotent application — applying the same transaction twice is a no-op,
   which is one half of exactly-once (the consumer supplies the other half).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .log import Record

CENT = Decimal("0.01")


@dataclass(frozen=True)
class Posting:
    account: str
    amount: Decimal  # positive = credit, negative = debit; Decimal via str only


def transaction(tx_id: str, postings: list[tuple[str, str]]) -> dict[str, Any]:
    """Build a transaction event. Rejected at construction if unbalanced —
    an unbalanced transaction must be impossible to write, not merely
    discouraged."""
    parsed = [Posting(acct, Decimal(amt)) for acct, amt in postings]
    if not parsed:
        raise ValueError("transaction needs at least one posting")
    total = sum(p.amount for p in parsed)
    if total != 0:
        raise ValueError(f"unbalanced transaction {tx_id}: postings sum to {total}")
    if any(p.amount.quantize(CENT) != p.amount for p in parsed):
        raise ValueError(f"sub-cent precision in {tx_id}: round explicitly before posting")
    return {
        "type": "transaction",
        "tx_id": tx_id,
        "postings": [{"account": p.account, "amount": str(p.amount)} for p in parsed],
    }


@dataclass
class Ledger:
    """The projection: balances plus the applied-set plus the committed
    offset, as ONE state object. That unity is the design point — state and
    progress commit together or not at all (see consumer.py).
    """

    balances: dict[str, Decimal] = field(default_factory=dict)
    applied_tx_ids: set[str] = field(default_factory=set)
    committed_offset: int = 0

    def apply(self, record: Record) -> bool:
        """Apply one record. Returns True if it changed state, False if it
        was a duplicate (idempotent replay). Never raises on duplicates —
        redelivery is normal operation, not an error."""
        event = record.event
        if event.get("type") != "transaction":
            return False
        tx_id = event["tx_id"]
        if tx_id in self.applied_tx_ids:
            return False
        for posting in event["postings"]:
            acct = posting["account"]
            self.balances[acct] = self.balances.get(acct, Decimal(0)) + Decimal(posting["amount"])
        self.applied_tx_ids.add(tx_id)
        return True

    def balance(self, account: str) -> Decimal:
        return self.balances.get(account, Decimal(0))

    def trial_balance(self) -> Decimal:
        """Sum of all balances. Always exactly zero if the invariants hold —
        this is the one-line audit a double-entry system owes you."""
        return sum(self.balances.values(), Decimal(0))

    def snapshot(self) -> dict:
        return {
            "balances": {k: str(v) for k, v in sorted(self.balances.items())},
            "applied": sorted(self.applied_tx_ids),
            "committed_offset": self.committed_offset,
        }
