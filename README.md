# ledgerproof

**An event-sourced, double-entry ledger with exactly-once processing — small enough to read, rigorous enough to argue with.**

[![tests](https://github.com/vmagapu/ledgerproof/actions/workflows/ci.yml/badge.svg)](https://github.com/vmagapu/ledgerproof/actions)

I've run event-sourced financial systems in production — underwriting, fraud detection, and regulatory reporting on Kafka streams, where transactional accuracy was contractually binding and "we think it processed once" was not an acceptable sentence. This repo is the architecture of those systems reduced to its load-bearing walls, with the claims that usually live in design docs turned into executable tests.

## The thesis

**"Exactly-once" is not a broker feature you enable — it is an end-to-end property you engineer from two halves:**

1. **Idempotent application.** The projection skips transactions it has already applied (dedup by `tx_id`). Redelivery is treated as normal operation, not an error — because with at-least-once delivery, it is.
2. **Atomic state + offset commit.** Balances and the consumer's committed offset persist as *one* unit. Commit them separately and a crash between the two either loses events or double-applies them; commit them together and a crash anywhere merely replays work that idempotency absorbs.

The test suite makes this argument executable rather than rhetorical: a `CrashPoint` kills the consumer **after state mutation but before commit** — the classic double-apply trap — at every offset in turn, and the recovered books are proven byte-identical to a crash-free run:

```python
with pytest.raises(SimulatedCrash):
    processor.run(crash=CrashPoint.AFTER_APPLY_BEFORE_COMMIT, crash_at_offset=1)
recovered = processor.run()          # offset 1 is redelivered
assert recovered.snapshot() == crash_free_baseline.snapshot()
```

## What else it demonstrates

- **Double entry as a constructive invariant.** An unbalanced transaction is *unwritable* — rejected at construction, not caught at reconciliation. The audit a double-entry system owes you is one line: `trial_balance() == 0`, always.
- **Replay as the system of record.** The projection can be rebuilt from offset zero at any time, and a test proves replay ≡ incremental processing. The event log, not the balances table, is the truth; a bug's blast radius is bounded by "fix the code, replay the log."
- **Tamper-evident history.** Every record carries a hash chained to its predecessor. Editing any historical event breaks verification from exactly that point — append-only enforced by mathematics, not by policy. Corrections are compensating events, never edits: history is evidence, and evidence is not rewritten.
- **Money is `Decimal`, constructed from strings, with sub-cent precision rejected at the boundary.** Same rule as everything I ship.

## Why no Kafka?

Deliberately. The broker is the easy part to swap in; the *semantics* are the hard part to get right, and they are broker-independent. `EventLog` keeps exactly what matters from a real topic — immutable ordered records, offsets, at-least-once redelivery to uncommitted consumers — in ~60 lines with zero dependencies, so every claim above runs as a deterministic test in milliseconds. Port `StateStore.commit` to a transactional database write (or Kafka's transactional producer + changelog) and the design carries over intact. If a design only works with a specific broker, it isn't a design — it's a dependency.

## Install

```bash
pip install git+https://github.com/vmagapu/ledgerproof   # zero dependencies
```

## The family

[finance-engines-mcp](https://github.com/vmagapu/finance-engines-mcp) · [evalgate](https://github.com/vmagapu/evalgate) · [modelgate](https://github.com/vmagapu/modelgate) · [ragproof](https://github.com/vmagapu/ragproof) · [agentic-sdlc-playbook](https://github.com/vmagapu/agentic-sdlc-playbook) — small, zero-dependency pieces of one philosophy: judgment to the model, numbers to deterministic code, everything measured — and in financial systems, everything *provable*.

— [Vish Magapu](https://vmagapu.github.io) · MIT
