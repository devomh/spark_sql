# Spark SQL And Databricks Glossary

This glossary uses the terms needed for the planned Spark SQL plan-analysis lab.

## Core Spark Terms

SparkSession: Main entry point for Spark SQL and DataFrame work. In PySpark, `spark` is usually a `SparkSession`.

Driver: The process that runs the user application, builds logical and physical plans, requests executors, and schedules tasks.

Cluster manager: The system that allocates resources to Spark applications. Examples include Spark Standalone, YARN, Kubernetes, and Databricks-managed infrastructure.

Spark master: The cluster manager process in Spark Standalone mode. It is not the same thing as the driver.

Worker: A machine or worker daemon that offers CPU and memory resources to Spark applications.

Executor: A JVM process launched for a Spark application on a worker. It runs tasks and stores shuffle/cache data.

Core: A CPU slot used by Spark to run tasks concurrently.

Partition: A slice of a distributed dataset. Spark usually schedules one task per partition per stage.

Task: The smallest unit of execution. A task processes one partition for one stage.

Job: Work triggered by an action such as `count`, `show`, `collect`, or `write`.

Stage: A group of tasks that can run without a shuffle boundary.

Shuffle: Data redistribution across executors, usually caused by joins, aggregations, distinct operations, and some sorts. In plans, this often appears as `Exchange`.

Action: An operation that triggers execution, such as `count`, `show`, `collect`, or `write`.

Transformation: A lazy operation that builds a plan, such as `select`, `filter`, `join`, or `groupBy`.

## Spark SQL Plan Terms

Parsed logical plan: The first representation of SQL or DataFrame operations. Names can still be unresolved.

Analyzed logical plan: Plan after Spark resolves tables, columns, functions, and types against the catalog.

Optimized logical plan: Logical plan after Catalyst optimizer rules, such as filter pushdown and projection pruning.

Physical plan: Concrete execution strategy using operators such as scans, joins, aggregates, exchanges, and sorts.

Catalyst optimizer: Spark SQL optimizer that analyzes and rewrites logical plans and selects physical plans.

Cost-Based Optimizer, or CBO: Spark optimizer behavior that uses table and column statistics to estimate row counts and sizes.

Adaptive Query Execution, or AQE: Runtime re-optimization based on statistics collected during query execution.

`EXPLAIN EXTENDED`: Shows parsed, analyzed, optimized, and physical plans.

`EXPLAIN COST`: Shows optimized logical plan with statistics when available.

`EXPLAIN FORMATTED`: Shows a physical-plan outline plus operator details.

`DataFrame.explain(mode="cost")`: PySpark equivalent for cost-oriented plan output.

Statistics: Estimated metadata such as `sizeInBytes` and `rowCount`.

`ANALYZE TABLE`: SQL command used to compute table or column statistics for the catalog.

## Common Physical Operators

Scan: Reads data from a source such as Parquet, Delta, CSV, or an in-memory relation.

Filter: Applies a row predicate, usually from a SQL `WHERE` clause.

Project: Selects or computes columns.

HashAggregate: Aggregates rows using hash-based grouping.

SortAggregate: Aggregates rows after sorting by grouping keys.

Exchange: Redistributes data across partitions. Usually indicates a shuffle.

BroadcastExchange: Sends a small relation to all executors so a join can avoid shuffling the large relation.

BroadcastHashJoin: Join strategy where the small side is broadcast and hashed locally.

SortMergeJoin: Join strategy where both sides are shuffled and sorted by join keys. Often expensive for large tables.

WholeStageCodegen: Spark optimization that combines multiple physical operators into generated JVM code.

AdaptiveSparkPlan: Physical-plan wrapper showing AQE is active.

## Data Warehouse Terms

Fact table: Large central table containing measurable events, such as sales transactions.

Dimension table: Smaller descriptive table joined to a fact table, such as product, customer, date, or store.

Star schema: Warehouse design with one central fact table and multiple dimension tables.

Grain: The level of detail represented by one fact-table row, such as one sale line item.

Measure: Numeric value analyzed in a fact table, such as quantity, revenue, or discount.

Surrogate key: Artificial key used to join fact and dimension tables.

Cardinality: Number of distinct values in a column.

Skew: Uneven distribution where some keys or partitions are much larger than others.

## Databricks Terms

Workspace: Databricks environment containing notebooks, jobs, clusters, SQL assets, and settings.

Notebook: Interactive document that runs code and SQL on Databricks compute.

Classic compute: User-configurable Databricks cluster with driver and worker nodes.

Serverless compute: Databricks-managed compute where infrastructure is mostly hidden from the user.

SQL warehouse: Databricks compute endpoint for SQL workloads.

DBU: Databricks Unit, a billing unit for Databricks usage. Cloud infrastructure may be billed separately depending on setup.

Query History: Databricks UI area for reviewing executed SQL statements.

Query Profile: Databricks query visualization with operator-level metrics. It can be exported as JSON in supported cases.

Spark UI: Spark web interface for jobs, stages, SQL, storage, environment, and executor details.

Delta Lake: Storage layer commonly used with Databricks that adds transactions and table features on top of data files.

Unity Catalog: Databricks governance layer for catalogs, schemas, tables, permissions, and lineage.

