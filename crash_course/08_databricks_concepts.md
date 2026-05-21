# Lesson 8: Databricks Concepts

> **By the end of this lesson you will be able to:**
> *   Describe what a Databricks SQL Warehouse adds over a plain Spark cluster.
> *   Read a Databricks Query Profile and find the four diagnostic metrics that matter.
> *   Name the Delta features that change how you should reason about plans.

While Spark is the engine, Databricks is the platform. It adds features that make it easier to manage, secure, and **observe** Spark workloads.

## Databricks SQL (DBSQL) & Warehouses

A **SQL Warehouse** is a specialized compute resource optimized for SQL performance.
*   **Instant Compute:** No waiting for VMs to spin up (if using Serverless).
*   **Photon Engine:** A vectorized execution engine written in C++ that is significantly faster than standard Spark (JVM).
*   **Optimized for BI:** Best for tools like Tableau, Power BI, or the Databricks SQL Editor.

## Observability: The Query Profile

In standard Spark, you have the Spark UI (which is great but complex). Databricks adds the **Query Profile**, a visual, interactive version of the execution plan.

### Key Metrics to Watch in a Query Profile:
1.  **Duration:** How long each operator took. Look for the "hottest" operator.
2.  **Rows Read vs. Rows Output:** If an operator reads 1 billion rows but only outputs 10, that's an efficient filter. If it outputs more than it reads (e.g., a join), watch out for data duplication!
3.  **Spill to Disk:** If an executor runs out of memory (RAM) during a shuffle or sort, it writes temporary data to disk. **Spill is a major performance killer.**
4.  **Pruning:** Look for "Partitions Pruned" and "Files Pruned." If these are 0, your query is doing a full table scan.

---

## Governance: Unity Catalog (UC)

Unity Catalog is the governance layer for Databricks. For performance, UC is important because:
*   **Centralized Metadata:** It stores all table statistics in one place, shared across all clusters.
*   **Lineage:** You can see which upstream tables affected your query's performance.
*   **Security:** It handles permissions (GRANT/REVOKE) at the row and column level without complex Spark configurations.

---

## Storage: Delta Lake Features

Delta Lake is the default format on Databricks. Beyond standard Parquet, it adds performance features:

### 1. Liquid Clustering (or Z-Order)
Traditional partitioning (e.g., `BY year, month`) is rigid. **Liquid Clustering** allows you to cluster data by multiple columns (e.g., `id` and `date`) without creating thousands of small folders. This speeds up data skipping dramatically.

### 2. Auto-Optimize
Databricks automatically handles "small file problems" by coalescing data into larger, more efficient files during the write process.

### 3. Predictive I/O
Databricks uses machine learning to predict which data you will need next and pre-fetches it from storage, further reducing I/O latency.

---

## Summary: When to Use What?

*   **Use All-Purpose Clusters** for data engineering (Python/Scala) and complex ETL.
*   **Use SQL Warehouses** for pure SQL analysis, BI reporting, and dashboards.
*   **Use Serverless** to eliminate "warm-up" time and management overhead.

---
**Navigation:** [Previous: The Intervention Toolkit](07_intervention_toolkit.md) | [Next: Exercises](09_exercises.md)
