# Worked Example: Reading an `EXPLAIN COST` output (q001)

> **What you'll practice in this walkthrough:**
> *   Locate the `Statistics(...)` annotations in a real `EXPLAIN COST` file and read them.
> *   Trace how one wrong leaf estimate poisons every estimate above it.
> *   Tell a *missing* statistic from a *present-but-wrong* one, and connect both to a real planning decision.

This is the hands-on companion to **[Lesson 5: Cost and Statistics](../05_cost_and_statistics.md)**. Lesson 5 used a small invented snippet; here we read a real one, line by line.

## The source

This walkthrough reads a captured `EXPLAIN COST` artifact. For
self-containment, verbatim copies are pinned **next to this file** — so the
line numbers and estimates quoted below stay valid even if the source run is
regenerated or removed:

- **`q001_explain_cost.txt`** — used in Steps 1–8 (the run captured *with* table statistics).
- **`q001_before_stats_explain_cost.txt`** — used in the Appendix (the same query *before* `ANALYZE TABLE`).

They are exact copies of the originals under the Workstream 2 run:

```
workstream_02_colab_poc/artifacts/runs/20260521T034715Z_colab_poc/
  q001_monthly_category_revenue/explain_cost.txt              ->  q001_explain_cost.txt
  q001_monthly_category_revenue_before_stats/explain_cost.txt ->  q001_before_stats_explain_cost.txt
```

The query, `q001`, is a monthly revenue rollup: it joins a sales fact table to three dimension tables, filters to a 12-month window, groups by month / category / region, and sorts. The shape is a classic **star join** — one big `fact_sales`, three small dimensions.

Open `q001_explain_cost.txt` alongside this walkthrough.

---

## Step 1 — Two plans in one file; the costs are on the *logical* one

`explain_cost.txt` contains two blocks:

```
== Optimized Logical Plan ==
...
== Physical Plan ==
...
```

Only the **Optimized Logical Plan** carries `Statistics(...)` annotations. The cost estimates are attached during *logical* optimization — before Spark picks physical operators. The Physical Plan below is what actually runs, but it has no cost numbers. So for cost interpretation, **read the top block.**

## Step 2 — Anatomy of a `Statistics(...)` line

Every operator line in the logical plan ends with a `Statistics(...)`. For example, the `dim_product` table's line:

```
+- Relation spark_catalog.ws2_poc.dim_product[...] parquet, Statistics(sizeInBytes=150.4 KiB, rowCount=2.00E+3)
```

- **`sizeInBytes`** — the estimated size of the data *as Spark would process it* (not the compressed Parquet footprint on disk).
- **`rowCount`** — the estimated number of rows. `2.00E+3` is scientific notation for **2,000**.
- `rowCount` is sometimes **absent** — that means Spark has a size guess but no row estimate at all.

Everything here is an **estimate the optimizer made before running the query**. There is no time anywhere in this file — cost estimates are rows and bytes only (see Lesson 5, "Two kinds of cost").

## Step 3 — Read the leaves first

Cost estimates are built **bottom-up**: leaf tables first, then everything above propagates from them. So find the four `Relation` lines — the base tables — and read their statistics:

| Table | `Statistics(...)` on its `Relation` line | Plausible? |
|---|---|---|
| `fact_sales` | `sizeInBytes=1.0 B, rowCount=0` | **No** — 0 rows in 1 byte |
| `dim_date` | `sizeInBytes=62.7 KiB, rowCount=730` | Yes — 730 ≈ two years of dates |
| `dim_product` | `sizeInBytes=150.4 KiB, rowCount=2.00E+3` | Yes — 2,000 products |
| `dim_store` | `sizeInBytes=6.4 KiB, rowCount=120` | Yes — 120 stores |

Three of the four look fine. The fact table — the *big* one, the whole point of the query — claims to be **empty**.

## Step 4 — The anomaly: a *present-but-wrong* statistic

`fact_sales` is annotated `Statistics(sizeInBytes=1.0 B, rowCount=0)`. The query runs against `fact_sales` and produces a result (there is a `result_parquet/` next to this file). A truly empty table cannot produce revenue rows. **The statistic is wrong.**

Note *how* it is wrong. The `rowCount=` field is **present** — so a statistic exists in the catalog; it just reads `0`. (`sizeInBytes=1.0 B` is Spark's floor — it never prints `0 B`.) This is different from a *missing* statistic:

- **Missing** — no statistic at all; Spark has to invent one (see the Appendix).
- **Present but wrong** — a statistic exists, so Spark trusts it and never falls back. This is the more dangerous case.

A `rowCount=0` on a table the query clearly reads from is a red flag, not a fact. Whether it is stale (computed when the table was empty) or mis-computed, you must not reason from it.

## Step 5 — Watch the bad estimate propagate

Now follow `fact_sales` *up* the tree. Every operator on the fact path inherits the empty estimate:

```
+- Join Inner, (date_id = date_id), Statistics(sizeInBytes=1.0 B, rowCount=0)
   +- Filter (... NOT returned_flag ...), Statistics(sizeInBytes=1.0 B, rowCount=0)
      +- Relation ...fact_sales[...] parquet, Statistics(sizeInBytes=1.0 B, rowCount=0)
```

The `Filter` over `fact_sales` is `1.0 B, rowCount=0`. The first `Join` is `1.0 B, rowCount=0`. So are the next two joins, every `Project` between them, and — at the very top — the `Aggregate` and `Sort` (`Statistics(sizeInBytes=1.0 B)`).

This is the key lesson: **a join's output cardinality is estimated from its inputs.** Join anything to a relation Spark thinks has 0 rows, and the join output is estimated at 0. The dimension subtrees keep their real numbers (730, 2,000, 120) right up until they meet the fact path — then the join collapses the whole result to 0. One wrong leaf → the entire upper plan is wrong.

## Step 6 — How `Filter` and `Project` move the numbers

The dimension subtrees are worth a closer look, because their estimates *do* change down the tree. Take `dim_date`:

```
+- Project [date_id, year_month], Statistics(sizeInBytes=22.1 KiB, rowCount=730)
   +- Filter (year_month >= 2024-04 AND year_month <= 2025-03 ...), Statistics(sizeInBytes=62.7 KiB, rowCount=730)
      +- Relation ...dim_date[...] parquet, Statistics(sizeInBytes=62.7 KiB, rowCount=730)
```

Two things to notice:

- **`Project` shrank `sizeInBytes` but not `rowCount`** (62.7 KiB → 22.1 KiB, still 730 rows). A projection keeps fewer columns, so the rows get *narrower* — fewer bytes, same count.
- **`Filter` did *not* shrink `rowCount`** (730 → 730), even though `year_month BETWEEN '2024-04' AND '2025-03'` should drop roughly half of a 730-row, two-year calendar. Spark left the estimate untouched because it had no **column-level** histogram for `year_month` to estimate the range's selectivity. Table-level `rowCount` alone is not enough — this is exactly why Lesson 5 says to run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`, not just the table.

## Step 7 — From estimate to decision: who gets broadcast

Estimates are not trivia — the optimizer *acts* on them. The three dimensions are estimated at 62.7 KiB, 150.4 KiB and 6.4 KiB, all far below the 10 MB `autoBroadcastJoinThreshold`, so all three joins become broadcast joins. Look at the Physical Plan and you see three `BroadcastHashJoin`s.

But look *which side* is broadcast. In this file the joins read `BuildLeft`, and the `BroadcastExchange` wraps the **fact-derived** side:

```
+- BroadcastHashJoin [store_id], [store_id], Inner, BuildLeft, false
   :- BroadcastExchange ...
   :  +- Project [customer_id, store_id, ...]   <- the fact-derived stream, being broadcast
```

The optimizer broadcast the *fact* side — because it believes that side has 0 rows, making it the smallest thing in the plan. Had the `fact_sales` statistic been correct (a large table), Spark would have kept the fact table on the streaming side and broadcast the **dimensions** instead.

So the wrong statistic did not just mis-size the plan — it **flipped which side gets broadcast**. It happened to run fine here because the real POC data is small. On real volume, broadcasting a fact-derived stream you wrongly believe is empty is a textbook driver `OutOfMemoryError`.

## Step 8 — Estimate vs. reality

Everything in `explain_cost.txt` is an *estimate*. What actually happened — the **measured cost** — lives in the run's `metadata.json`: about **402 ms** wall clock, **1 task**, **0 bytes spilled**. The query really is tiny.

So the broken estimate cost nothing here — purely by luck of small data. The discipline to take away: a cost plan is only as trustworthy as its leaf statistics, and a `rowCount=0` that contradicts a produced result invalidates everything above it. Read estimates critically; confirm with measured cost.

---

## Appendix — the same query with **no** statistics

The run also captured `q001` *before* `ANALYZE TABLE` was run — pinned here as `q001_before_stats_explain_cost.txt`. Compare the `fact_sales` `Relation` line:

```
+- Relation ...fact_sales[...] parquet, Statistics(sizeInBytes=8.0 EiB)
```

`8.0 EiB` (8 exabytes) is not real data — and notice there is **no `rowCount` field at all**. With no catalog statistic, Spark plugs in `spark.sql.defaultSizeInBytes` (`Long.MaxValue`). The join estimates above it then *explode* instead of collapsing:

```
Join Inner, (date_id = date_id),  Statistics(sizeInBytes=1.83E+22 B)
Join Inner, (product_id = ...),   Statistics(sizeInBytes=2.50E+26 B)
Join Inner, (store_id = ...),     Statistics(sizeInBytes=6.14E+29 B)
```

Two runs, two opposite-looking failures, **one root cause** — no usable `fact_sales` statistics:

| | `rowCount` field | `fact_sales` estimate | Failure mode |
|---|---|---|---|
| before stats | absent | `8.0 EiB` (sentinel) | estimate **explodes** |
| with stats | present, `= 0` | `1.0 B` | estimate **collapses** |

The fix is the same for both: `ANALYZE TABLE ws2_poc.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS`.

---

## Recap

1. Cost annotations live on the **Optimized Logical Plan**, never the Physical Plan.
2. A `Statistics(...)` line is **estimated** rows and bytes — never time.
3. Read **bottom-up**: leaf relations first, because every estimate above is derived from them.
4. An exabyte-scale `sizeInBytes` with no `rowCount` = a **missing** statistic; a `rowCount=0` on a table the query uses = a **present-but-wrong** one.
5. One bad leaf estimate poisons the whole upper plan — and can change real decisions like which side of a join gets broadcast.

---
**Navigation:** [Back to Lesson 5: Cost and Statistics](../05_cost_and_statistics.md) | [Crash Course Home](../README.md)
