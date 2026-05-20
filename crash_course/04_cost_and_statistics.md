# Lesson 4: Cost and Statistics

Spark's optimizer is only as good as the information it has. This information comes in the form of **Statistics**. Without them, the Cost-Based Optimizer (CBO) is basically "flying blind."

## The Magic Threshold: `spark.sql.autoBroadcastJoinThreshold`

This is one of the most important settings in Spark SQL.
*   **Default:** 10,485,760 bytes (10 MB).
*   **Behavior:** If Spark **knows** a table is smaller than this threshold, it will automatically use a **Broadcast Hash Join**. 
*   **The Catch:** Spark only knows the size if you tell it! If statistics are missing, Spark assumes the table is large and will default to a slow `SortMergeJoin`.

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

If you use **Delta Lake** (common on Databricks), many statistics are collected **automatically** when you write data. You rarely need to run `ANALYZE TABLE` on Delta tables, but it's still a good habit for traditional Parquet/CSV tables in a shared catalog.

---
**Navigation:** [Previous: Query Plan Reading](03_query_plan_reading.md) | [Next: Databricks Concepts](05_databricks_concepts.md)
