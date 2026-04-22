# Follow-ups

Parked work items to pick up later. Each entry has enough detail for a future
agent or future-you to resume cold.

---

## Decouple blocklist rules from category backtesting

**Status:** Not started
**Parked:** 2026-04-21
**Origin:** Surfaced during the `Trash As Learning Signal` plan (see
[.cursor/plans/trash_as_learning_signal_e3a6d62c.plan.md](.cursor/plans/trash_as_learning_signal_e3a6d62c.plan.md)).

### The problem

[src/miners.py](src/miners.py) `EngagementMiner` generates rules of the form:

```python
AutoRule(
    type="sender",
    pattern=...,
    classification="Newsletters",
    actions=["archive", "unsubscribe"],
    source=AutoRuleSource.ENGAGEMENT.value,
    ...
)
```

The intent is "bulk this sender out of the inbox regardless of topic". But
[src/backtester.py](src/backtester.py) validates every candidate by comparing
its `classification` field against historical labels in `processed_threads`.

Result: senders like USPS (historically labeled `Accounts`) or AI Tinkerers
(historically `Newsletters` but inconsistently) fail the backtest with 0-33%
accuracy because the miner hardcodes `"Newsletters"` for all of them.
All 5 blocklist candidates generated after the Trash backfill were rejected
for this reason.

### Evidence

Query after running `--learn-from-trash` + `--learn-now`:

```sql
SELECT rule_id, substr(pattern, 1, 50), classification, status,
       ROUND(backtest_accuracy, 2), source
FROM rule_candidates
WHERE source='engagement'
ORDER BY created_at DESC LIMIT 10;
```

All rows were `status=rejected` with `backtest_accuracy` 0.00 - 0.33.

### Two reasonable fixes

**Option A — Pure action rules (recommended).**
Drop `classification` from EngagementMiner rules. Add an `AutoRule.classification`
of `None` to mean "action-only". Update [src/classifier.py](src/classifier.py)
to honour `action-only` rules: match by sender, apply actions, preserve any
LLM/rule-derived category. Update [src/backtester.py](src/backtester.py) to
skip category-accuracy validation for rules without a classification and
instead validate the behavioral premise: `engagement_score <= 0.0 AND
total_received >= min_emails`.

**Option B — Engagement-aware backtester.**
Keep the rules as-is. Teach the backtester that `source=engagement` rules are
validated differently: re-query `sender_stats` for the rule's sender and check
that `engagement_score` is still `<= 0.0`. No category check. ~30 LOC in
[src/backtester.py](src/backtester.py).

Option A is cleaner architecturally; Option B is smaller.

### Scope / cost

- Option A: ~60-80 LOC across miners.py, classifier.py, backtester.py,
  models.py (nullable `classification`). Touches multiple modules.
- Option B: ~30 LOC in backtester.py only. Lower risk.

### Verification for either option

1. Re-run `--learn-now --mode local`.
2. Expect `auto_block_*` candidates with `source=engagement` to be
   `status=staged` or `status=active` (not `rejected`).
3. After staging window or on next learner run, they should auto-promote to
   `active` and start appearing in `config/auto_rules.yaml`.
4. New mail from USPS / AI Tinkerers / BITS AlmaConnect / Visible / Ameeth
   Kanawaday should be auto-archived + auto-unsubscribed on arrival.

### Where to start when resuming

Read [src/miners.py](src/miners.py) `EngagementMiner` (lines 100-131), then
[src/backtester.py](src/backtester.py) from top. Decide Option A vs B with
the user. Proceed in plan mode.
