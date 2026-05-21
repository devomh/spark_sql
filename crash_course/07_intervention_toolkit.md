# Lesson 7: The Intervention Toolkit

> **By the end of this lesson you will be able to:**
> *   Force a specific join strategy using SQL hints or DataFrame hints.
> *   Reshape partitions deliberately with `repartition`, `coalesce`, and the corresponding hints.
> *   Pick the right AQE / shuffle / broadcast config knob for the problem you're facing.

Catalyst + AQE handle the common case. When they don't — large skew that AQE misses, broadcast decisions you need to force, intermediate results with no usable stats — these are the levers you reach for.

This lesson is organised as a **decision table**: *if you see this symptom, try this intervention*.

---

## 1. Forcing Join Strategies

When the optimizer picks the wrong join type, use a **join hint** to override it.

### SQL syntax

```sql
SELECT /*+ BROADCAST(d) */ *
FROM   fact_sales s
JOIN   dim_product d ON s.product_id = d.id;
```

### DataFrame syntax

```python
from pyspark.sql.functions import broadcast

sales.join(broadcast(dim_product), "product_id")

# or, generically:
sales.join(dim_product.hint("broadcast"), "product_id")
```

### Available hints

| Hint | When to use it |
| :--- | :--- |
| `BROADCAST(t)` / `broadcast(df)` | You *know* `t` is small (< a few hundred MB) and Spark's stats disagree. |
| `MERGE(t)` | Force `SortMergeJoin` — useful if AQE incorrectly switched to broadcast and you got OOM. |
| `SHUFFLE_HASH(t)` | Force `ShuffledHashJoin` — faster than SMJ when one side is moderately small but too big to broadcast. |
| `SHUFFLE_REPLICATE_NL(t)` | Force a cartesian-style join. Rarely the right call. |

> [!WARNING]
> **Broadcast hints override the threshold.** If you `broadcast()` a 5 GB table, Spark will try, fail to fit it in driver memory, and crash. Hints are *advisory* to the optimizer but *binding* to the executor — verify the size first.

---

## 2. Controlling Partition Layout

### `repartition` vs. `coalesce`

| | `repartition(n, col?)` | `coalesce(n)` |
| :--- | :--- | :--- |
| Triggers a shuffle? | Yes (full) | No (merges existing partitions) |
| Can increase partitions? | Yes | No (only decrease) |
| Distributes evenly? | Yes | No (may be uneven) |
| Use it when | You want exactly `n` partitions or to redistribute by a key. | Cheap reduction (e.g., 200 → 4 before write). |

### Hint form

```sql
SELECT /*+ REPARTITION(50, customer_id) */ *
FROM events;

SELECT /*+ COALESCE(10) */ * FROM events;

SELECT /*+ REBALANCE(customer_id) */ *
FROM events;   -- AQE-aware: lets AQE choose the count for you
```

`REBALANCE` is usually the modern best choice: you say "spread evenly by this key" and let AQE pick the partition count.

---

## 3. The Knob Reference

The configs you will reach for most, in priority order.

### Join & broadcast

| Config | Default | What it changes |
| :--- | :--- | :--- |
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB) | Static threshold under which Spark broadcasts automatically. Set to `-1` to disable auto-broadcast entirely. |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | inherits above | AQE's runtime broadcast threshold. Raise carefully (driver memory). |
| `spark.sql.broadcastTimeout` | `300` (s) | Time to wait for a broadcast to finish. Bump if you see `BroadcastTimeout` errors on slow clusters. |

### Shuffle partitions

| Config | Default | What it changes |
| :--- | :--- | :--- |
| `spark.sql.shuffle.partitions` | `200` | Initial post-shuffle partition count. With AQE on, this is the *upper bound* — AQE coalesces down. |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64 MB` | AQE's target post-coalesce partition size. |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1 MB` | Partitions smaller than this are coalescing candidates. |

### Skew

| Config | Default | What it changes |
| :--- | :--- | :--- |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Master switch for AQE skew handling. |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | A partition is "skewed" if ≥ this × median. Lower (e.g., `3`) makes skew detection more aggressive. |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256 MB` | Absolute minimum size to qualify as skewed. Lower this for smaller queries where 256 MB is already huge. |

### CBO

| Config | Default | What it changes |
| :--- | :--- | :--- |
| `spark.sql.cbo.enabled` | `false` (OSS) | Turns on cost-based join reordering and selectivity estimation. Requires `ANALYZE TABLE` to be useful. |
| `spark.sql.cbo.joinReorder.enabled` | `false` | Allows the CBO to re-order joins for lower cost. Off by default because it adds planning time. |

---

## 4. Patterns by Symptom

### "I got an `OutOfMemoryError` on the driver during a join."
AQE or a hint tried to broadcast something too large.
*   Add an `MERGE(t)` hint to force `SortMergeJoin`.
*   Lower `spark.sql.adaptive.autoBroadcastJoinThreshold`.

### "One task runs for 30 minutes while the others finish in 30 seconds."
Classic skew that AQE didn't catch.
*   Lower `skewedPartitionFactor` to `3` and `skewedPartitionThresholdInBytes` to `64 MB`.
*   If that isn't enough, **salt the key**: append a random suffix to the skewed key on both sides, join on the salted key, then aggregate. Standard pattern; widely documented.

### "My output has 200 tiny files."
Too many shuffle partitions at write time.
*   `df.coalesce(N).write...` or add a `/*+ COALESCE(N) */` hint.
*   On Delta, run `OPTIMIZE table_name` after the write.

### "Spark is doing a `SortMergeJoin` on a dim table that I know is 30 MB."
Stats are missing or wrong.
*   Run `ANALYZE TABLE dim COMPUTE STATISTICS`.
*   If it's a view/CTE without stats, force it with `broadcast(dim)`.

### "My UDF is the slowest operator."
Catalyst can't see into it.
*   Rewrite as a native SQL/`pyspark.sql.functions` expression if possible.
*   If the logic genuinely needs Python, prefer a **Pandas UDF** (`@pandas_udf`) — vectorised, ~10× faster than row-at-a-time Python UDFs.
*   For numeric/string transforms, consider a Scala UDF registered from a JAR.

---

## 5. The Diagnostic Workflow

When a query is slow, work in this order:

1.  **`df.explain(mode="formatted")`** — what's the physical plan?
2.  **`df.explain(mode="cost")`** — do the row-count estimates match reality? If they're off by orders of magnitude, fix stats first.
3.  **Spark UI → SQL tab** — which stage is slow? Skew? Spill? Many tiny tasks?
4.  Pick the matching pattern above.
5.  **Re-run and re-explain.** Verify the plan actually changed.

Don't apply hints blindly. A hint that helped one query can wreck another when data sizes shift.

---
**Navigation:** [Previous: AQE Deep Dive](06_aqe_deep_dive.md) | [Next: Databricks Concepts](08_databricks_concepts.md)
