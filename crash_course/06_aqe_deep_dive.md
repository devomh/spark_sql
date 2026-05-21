# Lesson 6: Adaptive Query Execution (AQE) Deep Dive

> **By the end of this lesson you will be able to:**
> *   Explain *what* AQE can rewrite at runtime and *when* it gets a chance to.
> *   Name the three core AQE behaviours (join switching, partition coalescing, skew handling) and the configs that control each.
> *   Recognise AQE's fingerprints in a physical plan.

Lesson 1 explained why static planning is fragile in Spark. AQE is Spark's runtime answer: at every shuffle boundary, it pauses, measures what actually happened, and rewrites the rest of the plan with real numbers instead of estimates.

If Catalyst is the route Spark plans before the trip, AQE is the GPS that reroutes you when it sees the actual traffic.

---

## When AQE Runs

AQE is enabled by default in Spark 3.2+ (`spark.sql.adaptive.enabled = true`).

The trigger is simple: **after every shuffle (`Exchange`) completes**, AQE has fresh, exact statistics on the materialised shuffle data — partition sizes, row counts per partition, total bytes. It then:

1.  Looks at the remaining (unexecuted) part of the plan.
2.  Re-runs a subset of Catalyst's optimization rules using the new numbers.
3.  May substitute a different physical operator for the next stage.

The boundary matters: AQE can only re-plan *downstream* of a completed shuffle. A query with no shuffles gets no benefit from AQE.

---

## The Three Core AQE Behaviours

### 1. Dynamic Join Strategy Switching

**Trigger:** after a shuffle, AQE sees that one side of an upcoming join is now small enough to broadcast.

**Action:** cancel the planned `SortMergeJoin` and switch to `BroadcastHashJoin`.

**Knob:** `spark.sql.adaptive.autoBroadcastJoinThreshold` (defaults to the same 10 MB as the static threshold).

**Why it matters:** this attacks Challenge 2 (cardinality death spiral) head-on. The optimizer estimated 100 MB → SortMergeJoin; reality after the filter was 4 MB → broadcast wins.

### 2. Dynamic Partition Coalescing

**Trigger:** after a shuffle, AQE sees many shuffle partitions are tiny (the default `200` was too many for this query).

**Action:** logically merge consecutive small partitions into larger ones before the next stage reads them.

**Knobs:**
*   `spark.sql.adaptive.coalescePartitions.enabled` (default `true`)
*   `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default `64 MB`) — AQE aims for this size per coalesced partition
*   `spark.sql.adaptive.coalescePartitions.minPartitionSize` (default `1 MB`) — anything smaller is a coalescing candidate

**Why it matters:** attacks Challenge 5 (partition sizing). You no longer have to manually tune `spark.sql.shuffle.partitions` per query.

### 3. Dynamic Skew Handling

**Trigger:** after a shuffle, AQE notices one partition is dramatically bigger than the others.

**Action:** split the skewed partition into smaller subpartitions, and replicate the matching side of the join to keep results correct.

**Knobs:**
*   `spark.sql.adaptive.skewJoin.enabled` (default `true`)
*   `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (default `5`) — a partition is "skewed" if it's ≥ this many times the median
*   `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default `256 MB`) — *and* larger than this absolute size

**Why it matters:** attacks Challenge 3 (data skew). One straggler task no longer dictates job runtime.

---

## Spotting AQE in Plans

When you `df.explain()`, AQE shows itself in a few ways:

```text
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [id#10], [id#22], Inner
   :- ...
```

*   **`AdaptiveSparkPlan`** at the root means AQE is active.
*   **`isFinalPlan=false`** means you called `explain()` before execution finished. To see the *final* plan AQE arrived at, call `explain()` *after* a `.collect()` / `.show()` or use the Spark UI.
*   **`CustomShuffleReader`** appears where AQE has coalesced or split partitions.
*   **A `BroadcastHashJoin` where you expected `SortMergeJoin`** — and you didn't add a hint — is usually AQE's join switch in action.

---

## What AQE Does NOT Do

AQE is powerful but not omniscient:

*   **No initial-plan rescue.** If your first stage already OOMs because of skew on the scan side (no shuffle yet), AQE never gets a turn.
*   **No UDF insight.** It still can't reason about what a Python UDF does.
*   **No new stats for source tables.** It only learns from materialised shuffles. Stats on Parquet/Delta sources still matter.
*   **No automatic broadcast of large tables.** If a side is genuinely 5 GB, AQE will not try to broadcast it.

When AQE can't save you, you reach for the **[Intervention Toolkit](07_intervention_toolkit.md)** in the next lesson.

---
**Navigation:** [Previous: Cost and Statistics](05_cost_and_statistics.md) | [Next: The Intervention Toolkit](07_intervention_toolkit.md)
