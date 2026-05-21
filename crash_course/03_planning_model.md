# Lesson 3: Spark SQL Planning Model

> **By the end of this lesson you will be able to:**
> *   Walk through the four stages of the Catalyst lifecycle from SQL string to physical plan.
> *   Distinguish syntactic, semantic, and optimization errors by where they originate.
> *   Recognise predicate pushdown in a plan and explain why it matters.

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
*   **What it is:** The actual strategy for execution (e.g., "read Parquet files, use SortMergeJoin, hash partition by 200").
*   **Selection:** Spark applies a set of **strategy rules** (e.g., `JoinSelection`) that map each logical operator to one or more physical operators. With the CBO enabled, candidate plans are compared by estimated cost; without CBO, Spark uses simpler heuristics (broadcast if `sizeInBytes` is below the threshold, otherwise SortMergeJoin).

> [!NOTE]
> Spark's CBO is rule-driven, not full enumerative search like Cascades-based optimizers (SQL Server, Calcite). It doesn't explore every possible plan shape — it makes locally optimal decisions guided by stats. This is fast but means the *quality of your statistics matters more than the cleverness of the optimizer*.

---

## The Catalyst Rule: Predicate Pushdown

This is one of the most important optimizations to verify in your plans.

*   **Bad:** Read 1 TB of data → Filter out 99.9% of it.
*   **Good:** Tell the data source (Parquet/Delta) to only send the 0.1% of data that matches the filter.

In an `EXPLAIN` output, look for the **Scan** operator and check the `PushedFilters` attribute. If your filter isn't there, Spark is reading more data than it needs to.

---

## A Note on AQE

Catalyst produces a *static* plan based on whatever stats are available at planning time. In modern Spark (3.x+), **Adaptive Query Execution (AQE)** then layers a runtime feedback loop on top: after each shuffle, Spark inspects the actual data and may rewrite the rest of the plan.

If you see `AdaptiveSparkPlan` at the top of your physical plan, AQE is in play. The mechanics of *what* it can rewrite and *when* it triggers get a full treatment in **[Lesson 6: AQE Deep Dive](06_aqe_deep_dive.md)**.

---
**Navigation:** [Previous: Spark Execution Model](02_execution_model.md) | [Next: Query Plan Reading](04_query_plan_reading.md)
