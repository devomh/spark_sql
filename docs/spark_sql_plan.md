# Spark SQL And Databricks Learning Plan

This document turns `docs/brainstorm01.md` into an implementation-oriented plan. The goal is to learn Spark SQL and Databricks terms, build a first Colab proof of concept, then repeat the experiment in Azure without spending beyond free credits or free tiers.

## Current Reference Points

- Apache Spark SQL supports `EXPLAIN [EXTENDED | CODEGEN | COST | FORMATTED]` for logical and physical plans.
- PySpark `DataFrame.explain(mode=...)` supports `simple`, `extended`, `codegen`, `cost`, and `formatted`.
- Spark Standalone can run a master and one or more workers on one machine for testing.
- Azure free accounts currently advertise a $200 credit for up to 30 days.
- Azure Databricks currently has two relevant entry points:
  - Free trial: full platform evaluation with usage credits valid for 14 days, up to $400 in credits.
  - Free Edition: no payment, serverless only, daily usage limits, no classic compute.

Sources checked on 2026-05-20:

- Spark SQL `EXPLAIN`: https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-explain.html
- PySpark `DataFrame.explain`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
- Spark Standalone mode: https://dlcdn.apache.org/spark/docs/3.5.3/spark-standalone.html
- Spark Web UI: https://spark.apache.org/docs/3.5.6/web-ui.html
- Azure free account: https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account
- Azure Databricks free trial vs Free Edition: https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition
- Azure Databricks Free Edition limitations: https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations
- Databricks query profile JSON export: https://docs.databricks.com/aws/en/sql/user/queries/query-profile

## Decisions From The Brainstorm

- Use PySpark for the first POC, but run the important workloads as Spark SQL strings through `spark.sql(...)`. This gives a raw SQL experience while keeping Colab setup simple.
- Avoid Python row UDFs in the learning path. They introduce Python/JVM serialization behavior that distracts from Spark SQL plan analysis.
- Treat the MapReduce analogy as conceptual only. Spark stages often look like map and reduce phases around a shuffle, but Spark is not limited to the old Hadoop MapReduce execution model.
- Prefer generated warehouse data over uploaded CSVs for the first lab. This makes the experiment repeatable and lets us control table sizes, cardinality, and skew.
- Capture public plan outputs first. Internal PySpark/JVM query execution calls can be useful later, but they should not be the only artifact format.
- Do not carry forward the generated `get_init_data()` snippet from the brainstorm. It is not a valid SparkSession setup pattern.

## Terminology Correction

The requested "1 Driver, 2 Workers" setup is close, but the exact Spark terms depend on deployment mode.

For the Colab proof of concept:

- Spark master: the Standalone cluster manager process. It tracks workers and available resources.
- Driver: the PySpark application process started by the Colab notebook. It owns the `SparkSession`, builds plans, and schedules jobs.
- Workers: two Spark worker daemons started on the same Colab VM.
- Executors: JVM processes launched on workers for the application. Executors run tasks and hold shuffle/cache data.

So the precise emulation is:

`1 Spark master + 2 worker daemons + 1 notebook driver application + executors launched on the workers`.

For Azure Databricks classic compute:

- Driver node: the cluster VM running the Spark driver.
- Worker nodes: VMs running executors.
- Databricks workspace: control-plane environment for notebooks, jobs, clusters, SQL warehouses, and governance.

For Databricks Free Edition:

- Compute is serverless and quota-limited.
- It is useful for Spark SQL and plan/query-profile learning, but it does not expose a hand-built 1 driver / 2 worker classic cluster.

## Workstream 1: Crash Course

Purpose: learn Spark SQL and Databricks enough to read plans, reason about cost, and avoid wrong mental models.

Recommended lessons:

1. Spark execution model
   - `SparkSession`, driver, master, worker, executor, core, partition, task.
   - Lazy evaluation: transformations build a plan; actions trigger execution.
   - Jobs, stages, tasks, and shuffle boundaries.

2. Spark SQL planning model
   - Parsed logical plan.
   - Analyzed logical plan.
   - Optimized logical plan.
   - Physical plan.
   - Adaptive Query Execution, commonly abbreviated AQE.

3. Query-plan reading
   - Read physical plans bottom-up.
   - Identify `Scan`, `Filter`, `Project`, `HashAggregate`, `SortAggregate`, `Exchange`, `BroadcastExchange`, `BroadcastHashJoin`, `SortMergeJoin`, `AdaptiveSparkPlan`.
   - Treat `Exchange` as a major warning sign because it usually means a shuffle.

4. Cost and statistics
   - `EXPLAIN COST`.
   - `Statistics(sizeInBytes=..., rowCount=...)`.
   - Catalog statistics through `ANALYZE TABLE`.
   - Limits of estimates when stats are absent, stale, or based on temporary views.

5. Databricks concepts
   - Workspace, notebook, cluster/classic compute, serverless compute, SQL warehouse.
   - DBU, cloud VM cost, auto-termination.
   - Query Profile, Spark UI, Query History.
   - Delta Lake and Unity Catalog as later topics, not blockers for the first POC.

Outputs:

- `docs/spark_sql_glossary.md` as a vocabulary reference.
- A notebook section titled "Crash Course Exercises" with 6 to 8 small queries and guided plan observations.

## Workstream 2: Colab Proof Of Concept

Purpose: build a low-cost, reproducible Spark SQL lab that creates star-schema data, runs benchmark queries, saves query plans and runtime metrics, and prepares artifacts for later LLM analysis.

### POC Scope

Use generated synthetic data based on a simple warehouse model:

- Fact table: `fact_sales`
  - `sale_id`, `date_id`, `customer_id`, `product_id`, `store_id`, `quantity`, `unit_price`, `discount_amount`, `net_amount`
- Dimensions:
  - `dim_date`
  - `dim_customer`
  - `dim_product`
  - `dim_store`

Start small enough for Colab:

- Fact rows: 100k to 1M.
- Dimension rows: hundreds to tens of thousands.
- File format: Parquet.
- Partitioning experiment: write `fact_sales` partitioned by a low-cardinality date field such as `year_month`.

### Colab Cluster Shape

Implementation target:

- Install a pinned Spark distribution and Java.
- Start Spark Standalone master.
- Start two workers on localhost using distinct worker ports and worker directories.
- Configure event logging to a local directory.
- Create the `SparkSession` with `.master("spark://localhost:7077")`.
- Keep `spark.sql.shuffle.partitions` intentionally small, such as `4` or `8`, so stages are readable.

Important limitation:

- Colab runs on one VM, so this is process-level emulation, not real multi-host networking. It is still useful for seeing plans, stages, tasks, shuffles, and executor behavior.

### Query Set

The POC should include queries that intentionally create different plan shapes:

1. Filter and aggregate
   - Monthly revenue by product category.
   - Expected plan features: scan, filter pushdown where possible, partial aggregate, exchange, final aggregate.

2. Star join
   - Sales joined to product, customer, date, and store dimensions.
   - Expected plan features: broadcast joins for small dimensions, or sort-merge joins if broadcast is disabled.

3. Cost comparison
   - Same query before and after table statistics.
   - Expected plan features: improved `rowCount` and `sizeInBytes` estimates after `ANALYZE TABLE`.

4. Shuffle sensitivity
   - Same aggregation with different `spark.sql.shuffle.partitions`.
   - Expected output: visible changes in stage/task counts and shuffle partition behavior.

5. Join strategy experiment
   - Force or discourage broadcast with config and/or hints.
   - Expected output: compare `BroadcastHashJoin` vs `SortMergeJoin`.

6. Skew experiment
   - Generate a skewed product or customer key.
   - Expected output: task duration imbalance and, if AQE detects it, skew handling indicators.

### Plan Capture

For each query, save both raw and structured artifacts.

Raw files:

- `explain_simple.txt`
- `explain_extended.txt`
- `explain_cost.txt`
- `explain_formatted.txt`
- `sql_explain_cost.txt`
- `executed_plan_after_action.txt`

Structured file:

- `metadata.json`

Recommended `metadata.json` shape:

```json
{
  "run_id": "2026-05-20T120000Z_colab_q001",
  "environment": "colab-standalone",
  "spark_version": "3.5.x",
  "query_id": "q001_monthly_category_revenue",
  "query_sql": "SELECT ...",
  "tables": ["fact_sales", "dim_product", "dim_date"],
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.cbo.enabled": "true"
  },
  "plan_files": {
    "extended": "explain_extended.txt",
    "cost": "explain_cost.txt",
    "formatted": "explain_formatted.txt"
  },
  "static_indicators": {
    "exchange_count": 0,
    "broadcast_join_count": 0,
    "sort_merge_join_count": 0,
    "full_scan_count": 0,
    "estimated_size_in_bytes_max": null,
    "estimated_row_count_max": null
  },
  "runtime_indicators": {
    "wall_clock_ms": null,
    "job_count": null,
    "stage_count": null,
    "task_count": null,
    "shuffle_read_bytes": null,
    "shuffle_write_bytes": null,
    "spill_bytes": null,
    "max_task_duration_ms": null,
    "median_task_duration_ms": null
  },
  "alerts": []
}
```

Notes:

- Use public `EXPLAIN` output as the primary artifact because it is portable across Colab, Databricks, and plain Spark.
- Use Spark event logs or Spark UI data for runtime metrics when available.
- If using PySpark JVM internals such as `df._jdf.queryExecution()`, wrap them in a helper and document them as version-sensitive.

### Directory Layout

Suggested POC output layout:

```text
artifacts/
  runs/
    2026-05-20_colab_001/
      environment.json
      data_profile.json
      q001_monthly_category_revenue/
        query.sql
        explain_extended.txt
        explain_cost.txt
        explain_formatted.txt
        executed_plan_after_action.txt
        metadata.json
      q002_star_join/
        ...
```

### Threshold And Alert Rules

Add a simple rules file so costly plans can be flagged before any LLM step.

Initial rules:

- Alert if `Exchange` count is greater than 2.
- Alert if `SortMergeJoin` appears where a dimension table should broadcast.
- Alert if estimated `sizeInBytes` exceeds a configured budget.
- Alert if `max_task_duration_ms / median_task_duration_ms` is greater than 5.
- Alert if shuffle read or write bytes exceed a configured budget.
- Alert if spill bytes are greater than 0.
- Alert if a query contains a scan without a filter for large fact-table queries.

The first version can be regex-based over plan text plus parsed event-log metrics. A later version can use richer parsing or Databricks query profile JSON.

## Workstream 3: Azure Implementation Guide

Purpose: repeat the POC in Azure while controlling cost and preserving comparable artifacts.

### Cost Guardrails

Before creating resources:

- Use a new Azure free account only if eligible.
- Confirm the $200 / 30 day Azure free credit offer in the region/account flow.
- Create a budget alert in Microsoft Cost Management.
- Use a dedicated resource group, for example `rg-spark-sql-plan-lab`.
- Tag every resource with `project=spark-sql-plan-lab`.
- Delete the resource group after the experiment.
- Avoid always-on services unless they are free or explicitly budgeted.

For Databricks:

- Prefer Databricks Free Edition first if the goal is SQL, notebooks, serverless compute, and Query Profile JSON.
- Use Azure Databricks Free Trial only if classic compute or full platform behavior is needed.
- For a classic 1 driver / 2 worker shape, use the trial/full platform path, choose the smallest reasonable VM family available in-region, and set auto-termination to 10 or 15 minutes.
- Stop clusters manually after each run.

### Azure Path A: Databricks Free Edition

Use when:

- You want no payment requirement.
- You can accept serverless-only compute and daily usage limits.
- You mainly need notebooks, Spark SQL, Query History, Query Profile, and JSON export.

Limitations:

- No custom classic cluster shape.
- No explicit 1 driver / 2 worker cluster.
- Compute and outbound network access are limited.

Expected output:

- Same SQL queries as Colab.
- `EXPLAIN` text files saved from notebooks.
- Query Profile JSON downloaded when available.

### Azure Path B: Azure Databricks Free Trial Or Azure Free Credits

Use when:

- You need classic compute behavior closer to a real driver/worker cluster.
- You want Spark UI behavior and cluster controls.
- You accept that credits are time-limited and must be actively monitored.

Recommended minimal flow:

1. Create resource group.
2. Create Azure Databricks workspace.
3. Create storage:
   - Prefer ADLS Gen2 only if needed for cloud-storage practice.
   - Otherwise use workspace files or DBFS for the smallest first run.
4. Create classic all-purpose cluster:
   - Single driver plus two workers, if credits allow.
   - Auto-termination: 10 to 15 minutes.
   - Use a current LTS Databricks Runtime.
5. Upload or generate the same star-schema data.
6. Run the same query suite.
7. Save artifacts to a known folder.
8. Export Query Profile JSON where available.
9. Stop cluster.
10. Delete the resource group when finished.

### Azure Artifact Parity

Every Azure query should preserve the same core fields used by Colab:

- Query SQL.
- Spark/Databricks runtime version.
- Spark configuration.
- Plan text in `extended`, `cost`, and `formatted` mode.
- Final executed plan after action, when accessible.
- Runtime metrics from Spark UI, Query Profile, or event logs.
- Alerts generated by the same threshold rules.

## Workstream 4: LLM-Ready Plan Interpretation

Purpose: prepare plan artifacts so an LLM can later explain Spark plans in plain language and suggest tuning options.

### Non-LLM First

Before adding an LLM, build deterministic extraction:

- Count expensive operators: `Exchange`, `Sort`, `SortMergeJoin`, `BroadcastExchange`, `BroadcastHashJoin`.
- Extract estimated statistics: `sizeInBytes`, `rowCount`.
- Extract runtime metrics: duration, tasks, shuffle, spill, skew ratio.
- Emit alerts with clear rule names.

This gives the LLM reliable context instead of asking it to infer everything from raw text.

### LLM Input Package

For each query, provide the LLM:

- SQL text.
- Short table descriptions.
- Table row counts and file sizes.
- Parsed indicator summary.
- Alerts.
- Raw plan text, preferably trimmed to relevant sections.
- Runtime metrics.
- Spark and Databricks version.

### LLM Output Contract

Require the LLM to produce:

- Plain-language explanation.
- Main expensive operators.
- Why the operator is expensive.
- Confidence level.
- Suggested experiments.
- Warnings about missing statistics or uncertain estimates.

Example output shape:

```json
{
  "summary": "This query scans fact_sales, joins small dimensions, then shuffles rows by product category for aggregation.",
  "expensive_parts": [
    {
      "operator": "Exchange",
      "reason": "Rows are repartitioned across executors for GROUP BY.",
      "evidence": "Exchange hashpartitioning(category, 4)"
    }
  ],
  "suggestions": [
    "Verify table statistics are available before trusting EXPLAIN COST.",
    "Check whether dim_product is broadcast."
  ],
  "confidence": "medium"
}
```

## Suggested Milestones

1. Documentation foundation
   - Finish glossary.
   - Build a short crash-course outline.
   - Define the first query suite and artifact schema.

2. Colab notebook skeleton
   - Install Spark.
   - Start Standalone master and two workers.
   - Create `SparkSession`.
   - Confirm Spark UI and worker registration.

3. Synthetic warehouse data
   - Generate dimensions and fact table.
   - Save as Parquet.
   - Register temp views or managed tables.
   - Run `ANALYZE TABLE` where possible.

4. Plan capture library
   - Save explain modes.
   - Save query SQL.
   - Save Spark config.
   - Save runtime timing.
   - Parse initial static indicators.

5. Query experiments
   - Run baseline queries.
   - Run cost/statistics comparison.
   - Run join strategy comparison.
   - Run skew comparison.

6. Alerting
   - Implement initial threshold rules.
   - Store alerts in each query's `metadata.json`.
   - Produce a run-level summary table.

7. Azure guide and dry run
   - Choose Free Edition or Free Trial path.
   - Reuse the same SQL and artifact schema.
   - Capture Databricks Query Profile JSON if available.

8. LLM explainer prototype
   - Feed one query artifact package to an LLM.
   - Compare LLM output against deterministic alerts.
   - Refine the prompt and output contract.

## Recommended First Implementation Order

Start with Colab, not Azure.

Reason:

- Colab gives quick iteration without cloud billing risk.
- The same SQL and artifact schema can move to Azure later.
- The main learning target is plan interpretation, not cloud provisioning.

After the Colab notebook can reliably create data, run queries, save plans, and produce alerts, move the exact same query suite to Databricks Free Edition or a carefully budgeted Azure Databricks trial.
