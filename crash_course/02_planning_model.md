# Lesson 2: Spark SQL Planning Model

Spark SQL uses the **Catalyst Optimizer** to transform your code into a physical execution plan. Understanding this lifecycle helps you identify where Spark might be making "guesses" vs. informed decisions.

## The Catalyst Lifecycle

### 1. Parsed Logical Plan
*   **What it is:** A tree representation of your query syntax.
*   **Verification:** None. Spark doesn't know if `table_a` exists yet.
*   **Errors:** Syntax errors (e.g., `SELECT * FROMM table`).

### 2. Analyzed Logical Plan
*   **What it is:** The plan after Spark consults the **Catalog** (Metastore).
*   **Action:** Resolves table names, column names, and data types. 
*   **Errors:** Semantic errors (e.g., `Column 'age' not found in table 'users'`).

### 3. Optimized Logical Plan
*   **What it is:** The "best" version of your query logic, independent of physical hardware.
*   **Catalyst Rules Applied:**
    *   **Predicate Pushdown:** Moving filters (`WHERE`) into the data source scan.
    *   **Column Pruning:** Dropping unused columns early.
    *   **Boolean Simplification:** Changing `(a AND b) OR (a AND c)` to `a AND (b OR c)`.

### 4. Physical Plan
*   **What it is:** The actual strategy for execution (e.g., "Use 4 executors, read Parquet files, use SortMergeJoin").
*   **Selection:** Spark generates multiple physical plans and selects the one with the lowest **Cost** (if CBO is enabled).

---

## Adaptive Query Execution (AQE)

In traditional databases, the plan is fixed once execution starts. In modern Spark (3.x+), **AQE** allows the plan to evolve based on runtime statistics.

### How AQE Works:
1.  Spark starts execution with an initial plan.
2.  Once a stage (e.g., a shuffle) finishes, Spark collects exact statistics (e.g., "This partition actually has 0 rows").
3.  Spark **re-optimizes** the remaining plan using these new facts.

### AQE Superpowers:
*   **Switching Join Strategies:** If a table was estimated to be 100MB but ends up being only 5MB after filtering, AQE can switch a slow `SortMergeJoin` to a fast `BroadcastHashJoin` mid-query.
*   **Coalescing Partitions:** If you have 200 shuffle partitions but they are all tiny, AQE will merge them into fewer, larger partitions to reduce task overhead.
*   **Skew Join Handling:** If one partition is 10x larger than others (data skew), AQE can split that partition into smaller chunks to prevent a single "straggler" task from slowing down the whole job.

> [!NOTE]
> **Spotting AQE in Plans:** If you see `AdaptiveSparkPlan` or `AQE: true` in your `EXPLAIN` output, you know Spark is watching and adjusting the execution.

---

## The Catalyst Rule: Predicate Pushdown

This is one of the most important optimizations to verify in your plans.

*   **Bad:** Read 1TB of data -> Filter out 99.9% of it.
*   **Good:** Tell the data source (Parquet/Delta) to only send the 0.1% of data that matches the filter.

In an `EXPLAIN` output, look for the **Scan** operator and check the `PushedFilters` attribute. If your filter isn't there, Spark is reading more data than it needs to.

---
**Navigation:** [Previous: Spark Execution Model](01_execution_model.md) | [Next: Query Plan Reading](03_query_plan_reading.md)
