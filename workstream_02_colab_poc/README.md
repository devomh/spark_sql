# Workstream 2: Colab Spark SQL Plan POC

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/devomh/spark_sql/blob/main/workstream_02_colab_poc/colab_runner.ipynb)

This folder contains a Colab-ready proof of concept for generating Spark SQL plans and saving them as structured artifacts for later analysis.

The POC uses PySpark as the runtime wrapper, but the workload is written as Spark SQL strings. This keeps the experience close to raw Spark SQL while still being practical in Colab.

You can use either:

- `colab_runner.ipynb` for a notebook entry point.
- `colab_poc.py` directly from a Colab code cell or terminal cell.

## What This Builds

- A local Spark Standalone cluster shape in Colab:
  - 1 Spark master process.
  - 2 Spark worker processes.
  - 1 notebook/PySpark driver application.
  - Executors launched by Spark on the worker processes.
- A synthetic star-schema warehouse:
  - `dim_date`
  - `dim_product`
  - `dim_customer`
  - `dim_store`
  - `dim_promotion`
  - `fact_sales`
- A query suite designed to produce interesting plans:
  - Joins.
  - Aggregations.
  - Grouping sets.
  - Window functions.
  - Broadcast join hints.
  - Skew-sensitive grouping.
- Saved plan artifacts:
  - `explain_simple.txt`
  - `explain_extended.txt`
  - `explain_cost.txt`
  - `explain_formatted.txt`
  - `sql_explain_cost.txt`
  - `executed_plan_after_action.txt`
  - `metadata.json`

## Colab Setup

Run these cells in Google Colab before launching the POC.

```bash
!apt-get update -qq
!apt-get install -y openjdk-17-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
!tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /content
!pip install -q findspark
```

```python
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.1-bin-hadoop3"
```

Upload this folder to Colab or clone the repo, then run:

```bash
!python /content/workstream_02_colab_poc/colab_poc.py \
  --start-standalone \
  --base-dir /content/spark_sql_plan_poc \
  --fact-rows 300000
```

If you are running from a cloned repository:

```bash
!python /content/spark_sql/workstream_02_colab_poc/colab_poc.py \
  --start-standalone \
  --base-dir /content/spark_sql_plan_poc \
  --fact-rows 300000
```

For a faster smoke test:

```bash
!python /content/spark_sql/workstream_02_colab_poc/colab_poc.py \
  --start-standalone \
  --base-dir /content/spark_sql_plan_poc_smoke \
  --fact-rows 50000
```

Add `--stop-standalone-after-run` if you do not want to inspect the Spark master and worker UIs after the script finishes.

## Local Smoke Test

If you already have Java and PySpark locally, you can run without starting Spark Standalone:

```bash
python workstream_02_colab_poc/colab_poc.py \
  --master "local[*]" \
  --base-dir /tmp/spark_sql_plan_poc \
  --fact-rows 50000
```

## Output Layout

The script writes artifacts under:

```text
<base-dir>/
  data/
  spark-events/
  worker-1/
  worker-2/
  warehouse/
  artifacts/
    runs/
      <run-id>/
        environment.json
        data_profile.json
        run_summary.csv
        run_summary.json
        q001_before_stats_monthly_category_revenue/
        q001_monthly_category_revenue/
        q002_customer_window_revenue/
        q003_grouping_sets_profitability/
        q004_broadcast_star_join_auto_broadcast/
        q004_broadcast_star_join_no_broadcast/
        q005_shuffle_partition_sensitivity_shuffle_4/
        q005_shuffle_partition_sensitivity_shuffle_16/
        q006_skewed_product_hotspots/
```

Each query directory contains the SQL text, multiple explain outputs, runtime metadata, and deterministic alerts.

## Important Limits

- Colab uses one VM. This emulates Spark worker processes but not real multi-host network behavior.
- Spark cost estimates are only as good as available table and column statistics.
- Runtime metrics (`shuffle_read_bytes`, `shuffle_write_bytes`, `spill_bytes`, `max_task_duration_ms`, `median_task_duration_ms`) are reconstructed from the Spark event log after each query. They depend on the event-log writer having flushed task-end events; tail events for very short stages may occasionally be missing.
- Internal calls such as `df._jdf.queryExecution()` are used only as helper capture mechanisms. The portable artifacts are the public `EXPLAIN` outputs.
