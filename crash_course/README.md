# Spark SQL Crash Course

Welcome to the Spark SQL Crash Course. This course is designed to help you understand how Spark executes queries, how to read execution plans, and how to optimize your workloads on Spark and Databricks.

## Course Syllabus

1.  **[Spark Execution Model](01_execution_model.md)**: Understand the hierarchy of execution from the SparkSession down to individual tasks and shuffle boundaries.
2.  **[Spark SQL Planning Model](02_planning_model.md)**: Learn about the Catalyst optimizer and the lifecycle of a query from SQL string to physical execution.
3.  **[Query Plan Reading](03_query_plan_reading.md)**: Identify key operators like `Scan`, `Exchange`, and various `Join` strategies in physical plans.
4.  **[Cost and Statistics](04_cost_and_statistics.md)**: See how table statistics and the Cost-Based Optimizer (CBO) influence Spark's decisions.
5.  **[Databricks Concepts](05_databricks_concepts.md)**: Transition from open-source Spark to the Databricks platform, covering SQL Warehouses and Query Profiles.
6.  **[Exercises](06_exercises.md)**: Practice what you've learned with guided plan analysis tasks.
7.  **[Why Spark Planning is Hard](07_planning_challenges.md)**: Explore the fundamental differences between Spark and traditional OLTP databases, and why Adaptive Query Execution (AQE) is critical.

## How to use this course

Each lesson builds on the previous one. If you are new to Spark, start at the beginning. If you are already familiar with Spark's architecture but want to dive deep into performance, you might jump straight to **Lesson 3: Query Plan Reading**.

For a quick reference of terms, see the [Spark SQL Glossary](../docs/spark_sql_glossary.md).

---
**Navigation:** [Next: Spark Execution Model](01_execution_model.md)
