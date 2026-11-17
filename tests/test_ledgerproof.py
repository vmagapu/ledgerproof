"""The proofs: invariants, crash-recovery equivalence, replay determinism,
and tamper evidence. Deterministic throughout."""

from decimal import Decimal

import pytest

from ledgerproof.consumer import CrashPoint, Processor, SimulatedCrash, StateStore, replay
from ledgerproof.ledger import Ledger, transaction
from ledgerproof.log import EventLog


def seeded_log():
    log = EventLog()
    log.append(transaction("tx-1", [("cash", "-100.00"), ("alice", "100.00")]))
    log.append(transaction("tx-2", [("alice", "-30.00"), ("bob", "30.00")]))
    log.append(transaction("tx-3", [("bob", "-5.00"), ("fees", "5.00")]))
    return log


# ---------- double-entry invariants ----------

def test_unbalanced_transaction_is_unwritable():
    with pytest.raises(ValueError, match="unbalanced"):
        transaction("bad", [("cash", "-100.00"), ("alice", "99.99")])


def test_sub_cent_precision_rejected():
    with pytest.raises(ValueError, match="sub-cent"):
        transaction("bad", [("a", "-0.005"), ("b", "0.005")])


def test_trial_balance_is_always_zero():
    ledger = replay(seeded_log())
    assert ledger.trial_balance() == Decimal("0")
    assert ledger.balance("alice") == Decimal("70.00")
    assert ledger.balance("bob") == Decimal("25.00")
    assert ledger.balance("fees") == Decimal("5.00")


# ---------- idempotency (half one of exactly-once) ----------

def test_duplicate_application_is_a_noop():
    log = seeded_log()
    ledger = Ledger()
    records = list(log.read_from(0))
    assert ledger.apply(records[0]) is True
    assert ledger.apply(records[0]) is False  # redelivery absorbed
    assert ledger.balance("alice") == Decimal("100.00")


# ---------- atomic commit (half two of exactly-once) ----------

def test_crash_free_run():
    log = seeded_log()
    final = Processor(log, StateStore(Ledger())).run()
    assert final.committed_offset == 3
    assert final.balance("alice") == Decimal("70.00")


def test_crash_after_apply_before_commit_does_not_double_apply():
    """The classic trap, made executable: crash lands after state mutation
    but before the commit, the record is redelivered on recovery, and the
    final books are IDENTICAL to a crash-free run."""
    log = seeded_log()
    store = StateStore(Ledger())
    processor = Processor(log, store)

    with pytest.raises(SimulatedCrash):
        processor.run(crash=CrashPoint.AFTER_APPLY_BEFORE_COMMIT, crash_at_offset=1)

    # durable state never saw the in-flight record
    assert store.durable.committed_offset == 1
    assert store.durable.balance("bob") == Decimal("0")

    recovered = processor.run()  # redelivers offset 1 and continues
    baseline = Processor(seeded_log(), StateStore(Ledger())).run()
    assert recovered.snapshot() == baseline.snapshot()


def test_repeated_crashes_still_converge():
    log = seeded_log()
    store = StateStore(Ledger())
    processor = Processor(log, store)
    for offset in (0, 1, 2):
        with pytest.raises(SimulatedCrash):
            processor.run(crash=CrashPoint.AFTER_APPLY_BEFORE_COMMIT, crash_at_offset=offset)
    final = processor.run()
    assert final.snapshot() == Processor(seeded_log(), StateStore(Ledger())).run().snapshot()


# ---------- replay & audit ----------

def test_replay_equals_incremental_processing():
    log = seeded_log()
    incremental = Processor(log, StateStore(Ledger())).run()
    assert replay(log).snapshot() == incremental.snapshot()


def test_new_events_after_recovery_are_processed_once():
    log = seeded_log()
    store = StateStore(Ledger())
    Processor(log, store).run()
    log.append(transaction("tx-4", [("cash", "-1.00"), ("fees", "1.00")]))
    final = Processor(log, store).run()
    assert final.balance("fees") == Decimal("6.00")
    assert final.committed_offset == 4


def test_hash_chain_detects_tampering():
    log = seeded_log()
    assert log.verify_chain() == 3
    # tamper with history (bypassing the public API, as an attacker would)
    tampered = log._records[1].event.copy()
    tampered["postings"] = [{"account": "alice", "amount": "-30.00"},
                            {"account": "mallory", "amount": "30.00"}]
    object.__setattr__(log._records[1], "event", tampered)
    assert log.verify_chain() == 1  # chain breaks exactly where history was edited


def test_appending_remains_valid_after_many_events():
    log = seeded_log()
    for i in range(50):
        log.append(transaction(f"bulk-{i}", [("cash", "-0.01"), ("fees", "0.01")]))
    assert log.verify_chain() == len(log)
    assert replay(log).trial_balance() == Decimal("0")
