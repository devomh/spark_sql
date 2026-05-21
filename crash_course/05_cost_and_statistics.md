# Lesson 5: Cost and Statistics

> **By the end of this lesson you will be able to:**
> *   List what stats Spark gets for free (file size) vs. what requires `ANALYZE TABLE` (row count, NDV, min/max).
> *   Explain how `spark.sql.autoBroadcastJoinThreshold` interacts with stats to choose a join strategy.
> *   Read a `EXPLAIN COST` output and identify whether the estimates are credible.

Spark's optimizer is only as good as the information it has. This information comes in the form of **Statistics**. Without them, the Cost-Based Optimizer (CBO) is basically "flying blind."

## The Magic Threshold: `spark.sql.autoBroadcastJoinThreshold`

This is one of the most important settings in Spark SQL.
*   **Default:** 10,485,760 bytes (10 MB). Some distributions (e.g., Databricks) ship with a higher default.
*   **Behavior:** If Spark **knows** a table is smaller than this threshold, it will automatically use a **Broadcast Hash Join**. 
*   **The Catch:** "Knows" depends on the source:
    *   **Parquet/Delta files:** `sizeInBytes` is read from file footers/manifests for free — Spark almost always has *some* estimate of the on-disk size.
    *   **`rowCount`, NDV, min/max:** these require `ANALYZE TABLE` for Parquet, or are auto-collected for Delta. Without them the CBO falls back to crude size-based heuristics.
    *   **Views, CTEs, and derived tables:** stats are propagated only when Spark can infer them — often it can't. The "intermediate result of a 3-way join" usually has no useful stats at all.

## Types of Statistics

When you run `ANALYZE TABLE`, Spark collects:

### 1. Table-Level Stats
*   **sizeInBytes**: Total size of the table on disk.
*   **rowCount**: Total number of rows. (Crucial for estimating join results).

### 2. Column-Level Stats
*   **distinctCount (NDV)**: Number of Distinct Values. Helps Spark estimate the "selectivity" of a filter.
*   **min / max**: Used for "min-max skipping" during scans.
*   **nullCount**: Number of null values.
*   **avgLen / maxLen**: Average and max length of strings.

```sql
-- The "Golden Command" for performance
ANALYZE TABLE fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Verifying Cost in the Plan

Use `EXPLAIN COST` or `df.explain(mode="cost")` to see what Spark is thinking.

```text
== Optimized Logical Plan ==
Join Inner, (id#1 = id#2)
:- Filter (amount#5 > 100), Statistics(sizeInBytes=500.0 KB, rowCount=1.00E+4)
:  +- Relation[id#1,amount#5] parquet, Statistics(sizeInBytes=10.0 GB, rowCount=1.00E+9)
+- Relation[id#2,name#6] parquet, Statistics(sizeInBytes=5.0 MB, rowCount=1.00E+5)
```

**What this tells us:**
1.  The base table is 10GB (1 billion rows).
2.  The filter `amount > 100` is estimated to reduce the data to 500KB (10,000 rows).
3.  Because 500KB is < 10MB threshold, Spark will likely choose a **Broadcast Join** for this filter.

---

## When Statistics Fail

Statistics can be "liars" if they aren't maintained:
*   **Stale Stats:** You deleted half the table, but the stats still say 1 billion rows. Spark might avoid a broadcast join it could have safely used.
*   **Temporary Views:** Statistics are often not preserved through complex views or CTEs unless Spark can infer them.
*   **UDFs:** Spark has no idea how a Python UDF will change the size of the data. It usually gives up and uses a generic estimate.

---

## Pro-Tip: The "Delta Advantage"

If you use **Delta Lake** (common on Databricks), file-level min/max for the first 32 columns and `numRecords` per file are collected **automatically** on write. This usually means Delta queries get good size and basic per-column stats without you doing anything. You still benefit from `ANALYZE TABLE ... FOR COLUMNS` for column-level NDV (used by selectivity estimation), and you should always run it for traditional Parquet/CSV tables in a shared catalog.

---
**Navigation:** [Previous: Query Plan Reading](04_query_plan_reading.md) | [Next: AQE Deep Dive](06_aqe_deep_dive.md)
