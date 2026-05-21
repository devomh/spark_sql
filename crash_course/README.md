# Spark SQL Crash Course

A self-paced introduction to how Spark plans and executes SQL, how to read execution plans, and how to steer the optimizer when it makes the wrong call.

## Prerequisites

You should already be comfortable with:
*   **SQL**: `SELECT`, `JOIN`, `GROUP BY`, subqueries.
*   **Python basics** (the examples use PySpark; equivalents in Scala/SQL are noted where relevant).
*   **The idea of a query optimizer** at a high level (you don't need to have used one before).

You do *not* need prior Spark experience. Lesson 1 motivates everything that follows.

## Setup

For the runnable examples and Lesson 9 exercises you need a working PySpark installation. See **[SETUP.md](SETUP.md)** — it walks through `pip install pyspark`, a minimal `SparkSession`, and your first `df.explain()` call.

If you only want to read the conceptual lessons, no setup is needed.

## Course Syllabus

| # | Lesson | What you'll get out of it |
| :--- | :--- | :--- |
| 1 | **[Why Spark Planning is Hard](01_why_planning_is_hard.md)** | The five core challenges (external storage, join estimation, skew, UDFs, partition sizing) that motivate everything else. |
| 2 | **[Spark Execution Model](02_execution_model.md)** | Driver / Executors / Jobs / Stages / Tasks, and why shuffles are expensive. |
| 3 | **[Spark SQL Planning Model](03_planning_model.md)** | Catalyst's lifecycle from SQL string to physical plan. |
| 4 | **[Query Plan Reading](04_query_plan_reading.md)** | Recognising `Scan`, `Exchange`, join strategies, and codegen markers in `EXPLAIN` output. |
| 5 | **[Cost and Statistics](05_cost_and_statistics.md)** | How table stats drive the Cost-Based Optimizer's decisions. |
| 6 | **[AQE Deep Dive](06_aqe_deep_dive.md)** | How Adaptive Query Execution rewrites the plan at runtime, with the exact triggers. |
| 7 | **[The Intervention Toolkit](07_intervention_toolkit.md)** | Hints, configs, and patterns to force the plan you want when the optimizer is wrong. |
| 8 | **[Databricks Concepts](08_databricks_concepts.md)** | SQL Warehouses, Photon, Query Profiles, Delta features. |
| 9 | **[Exercises (with answers)](09_exercises.md)** | Practice plan analysis and verify your understanding. |

## How to use this course

Lessons build on each other. If you are new to Spark, read them in order. If you already know the architecture, skim Lessons 2–3 and start at Lesson 4.

Each lesson begins with a short **"By the end of this lesson"** block so you know whether to read it carefully or skim.

For a quick reference of terms, see the [Spark SQL Glossary](../docs/spark_sql_glossary.md).

---
**Navigation:** [Next: Why Spark Planning is Hard](01_why_planning_is_hard.md)
