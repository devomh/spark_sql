#!/usr/bin/env python3
"""Colab Spark SQL plan-analysis proof of concept.

The script intentionally keeps the workload in Spark SQL strings while using
PySpark for setup, data generation, artifact capture, and metadata writing.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import time
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_QUERY_DIR = ROOT / "queries"
DEFAULT_ALERT_RULES = ROOT / "config" / "alert_rules.json"
DATABASE = "ws2_poc"
QUERY_CONF_VARIANTS = {
    "q004_broadcast_star_join": [
        ("auto_broadcast", {"spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024)}),
        ("no_broadcast", {"spark.sql.autoBroadcastJoinThreshold": "-1"}),
    ],
    "q005_shuffle_partition_sensitivity": [
        ("shuffle_4", {"spark.sql.shuffle.partitions": "4"}),
        ("shuffle_16", {"spark.sql.shuffle.partitions": "16"}),
    ],
}


@dataclass(frozen=True)
class PocConfig:
    base_dir: Path
    query_dir: Path
    alert_rules_path: Path
    master: str
    fact_rows: int
    products: int
    customers: int
    stores: int
    promotions: int
    shuffle_partitions: int
    run_id: str
    start_standalone: bool
    stop_standalone_after_run: bool
    skip_analyze: bool


def ensure_pyspark_importable() -> None:
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        try:
            import findspark  # type: ignore

            findspark.init(spark_home)
        except ImportError:
            pass


def start_background_process(name: str, command: list[str], log_path: Path, pid_path: Path, env: dict[str, str]) -> None:
    print("+ " + " ".join(command))
    with log_path.open("ab") as log_handle:
        process = subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    time.sleep(2)
    if process.poll() is not None:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
        raise RuntimeError(f"{name} exited during startup. Log tail:\n{tail}")


def start_standalone_cluster(base_dir: Path) -> None:
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        raise RuntimeError("SPARK_HOME must be set before starting Spark Standalone.")

    spark_class = Path(spark_home) / "bin" / "spark-class"
    master_url = "spark://localhost:7077"
    logs_dir = base_dir / "standalone-logs"
    pids_dir = base_dir / "standalone-pids"
    logs_dir.mkdir(parents=True, exist_ok=True)
    pids_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "SPARK_MASTER_HOST": "localhost",
            "SPARK_MASTER_PORT": "7077",
            "SPARK_MASTER_WEBUI_PORT": "8080",
            "SPARK_LOG_DIR": str(logs_dir),
        }
    )
    start_background_process(
        "spark-master",
        [
            str(spark_class),
            "org.apache.spark.deploy.master.Master",
            "--host",
            "localhost",
            "--port",
            "7077",
            "--webui-port",
            "8080",
        ],
        logs_dir / "spark-master.log",
        pids_dir / "spark-master.pid",
        env,
    )

    for index, web_port in enumerate((8081, 8082), start=1):
        worker_dir = base_dir / f"worker-{index}"
        worker_dir.mkdir(parents=True, exist_ok=True)
        worker_env = env.copy()
        start_background_process(
            f"spark-worker-{index}",
            [
                str(spark_class),
                "org.apache.spark.deploy.worker.Worker",
                "--webui-port",
                str(web_port),
                "--cores",
                "1",
                "--memory",
                "1g",
                "--work-dir",
                str(worker_dir),
                master_url,
            ],
            logs_dir / f"spark-worker-{index}.log",
            pids_dir / f"spark-worker-{index}.pid",
            worker_env,
        )


def stop_standalone_cluster(base_dir: Path) -> None:
    pids_dir = base_dir / "standalone-pids"
    for pid_name in ("spark-worker-2.pid", "spark-worker-1.pid", "spark-master.pid"):
        pid_path = pids_dir / pid_name
        if not pid_path.exists():
            continue
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
            os.kill(pid, signal.SIGTERM)
            print(f"Stopped {pid_name.removesuffix('.pid')} pid={pid}")
        except ProcessLookupError:
            print(f"Process already stopped for {pid_name}")
        except Exception as exc:
            print(f"Could not stop process from {pid_path}: {exc}")


def create_spark_session(config: PocConfig):
    from pyspark.sql import SparkSession

    event_log_dir = config.base_dir / "spark-events"
    warehouse_dir = config.base_dir / "warehouse"
    event_log_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.master(config.master)
        .appName("workstream-02-spark-sql-plan-poc")
        .config("spark.sql.warehouse.dir", warehouse_dir.as_uri())
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir.as_uri())
        .config("spark.sql.cbo.enabled", "true")
        .config("spark.sql.statistics.histogram.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "2g")
    )
    return builder.getOrCreate()


def reset_database(spark: Any, data_dir: Path) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    for table in (
        "fact_sales",
        "dim_date",
        "dim_product",
        "dim_customer",
        "dim_store",
        "dim_promotion",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table}")
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)


def generate_dim_date(spark: Any):
    from pyspark.sql import functions as F

    return (
        spark.range(0, 730)
        .withColumn("sale_date", F.date_add(F.to_date(F.lit("2024-01-01")), F.col("id").cast("int")))
        .withColumn("date_id", F.date_format("sale_date", "yyyyMMdd").cast("int"))
        .withColumn("calendar_year", F.year("sale_date"))
        .withColumn("fiscal_year", F.when(F.month("sale_date") >= 7, F.year("sale_date") + 1).otherwise(F.year("sale_date")))
        .withColumn("month_number", F.month("sale_date"))
        .withColumn("year_month", F.date_format("sale_date", "yyyy-MM"))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("sale_date"), F.lit("-"), F.year("sale_date")))
        .withColumn("day_of_week", F.date_format("sale_date", "E"))
        .withColumn("is_weekend", F.dayofweek("sale_date").isin([1, 7]))
        .select(
            "date_id",
            "sale_date",
            "calendar_year",
            "fiscal_year",
            "month_number",
            "year_month",
            "quarter_name",
            "day_of_week",
            "is_weekend",
        )
    )


def generate_dim_product(spark: Any, products: int):
    from pyspark.sql import functions as F

    categories = F.array(
        *[F.lit(value) for value in ("Audio", "Computers", "Home", "Kitchen", "Outdoor", "Toys", "Wellness", "Books")]
    )
    departments = F.array(*[F.lit(value) for value in ("Electronics", "Home Goods", "Lifestyle", "Media")])

    return (
        spark.range(1, products + 1)
        .withColumnRenamed("id", "product_id")
        .withColumn("category", F.element_at(categories, (F.pmod(F.col("product_id"), F.lit(8)) + 1).cast("int")))
        .withColumn("department", F.element_at(departments, (F.pmod(F.col("product_id"), F.lit(4)) + 1).cast("int")))
        .withColumn("brand_id", (F.pmod(F.col("product_id") * 13, F.lit(80)) + 1).cast("int"))
        .withColumn("is_premium", F.pmod(F.col("product_id"), F.lit(7)).isin([0, 1]))
        .withColumn("unit_price", F.round(F.lit(8.0) + F.pmod(F.col("product_id") * 17, F.lit(190)) + F.rand(11) * 20, 2))
        .withColumn("standard_cost", F.round(F.col("unit_price") * (F.lit(0.42) + F.rand(12) * 0.2), 2))
        .select("product_id", "category", "department", "brand_id", "is_premium", "unit_price", "standard_cost")
    )


def generate_dim_customer(spark: Any, customers: int):
    from pyspark.sql import functions as F

    segments = F.array(*[F.lit(value) for value in ("Consumer", "Small Business", "Enterprise", "Education")])
    regions = F.array(*[F.lit(value) for value in ("North", "South", "East", "West", "Central")])
    channels = F.array(*[F.lit(value) for value in ("Organic", "Paid Search", "Referral", "Retail", "Partner")])

    return (
        spark.range(1, customers + 1)
        .withColumnRenamed("id", "customer_id")
        .withColumn("segment", F.element_at(segments, (F.pmod(F.col("customer_id") * 3, F.lit(4)) + 1).cast("int")))
        .withColumn("region", F.element_at(regions, (F.pmod(F.col("customer_id") * 5 + 2, F.lit(5)) + 1).cast("int")))
        .withColumn("signup_channel", F.element_at(channels, (F.pmod(F.col("customer_id") * 7, F.lit(5)) + 1).cast("int")))
        .withColumn(
            "age_band",
            F.when(F.pmod(F.col("customer_id"), F.lit(5)) == 0, F.lit("18-24"))
            .when(F.pmod(F.col("customer_id"), F.lit(5)) == 1, F.lit("25-34"))
            .when(F.pmod(F.col("customer_id"), F.lit(5)) == 2, F.lit("35-44"))
            .when(F.pmod(F.col("customer_id"), F.lit(5)) == 3, F.lit("45-54"))
            .otherwise(F.lit("55+")),
        )
        .select("customer_id", "segment", "region", "signup_channel", "age_band")
    )


def generate_dim_store(spark: Any, stores: int):
    from pyspark.sql import functions as F

    regions = F.array(*[F.lit(value) for value in ("North", "South", "East", "West", "Central")])
    store_types = F.array(*[F.lit(value) for value in ("Flagship", "Mall", "Outlet", "Online")])

    return (
        spark.range(1, stores + 1)
        .withColumnRenamed("id", "store_id")
        .withColumn("region", F.element_at(regions, (F.pmod(F.col("store_id") * 11, F.lit(5)) + 1).cast("int")))
        .withColumn("store_type", F.element_at(store_types, (F.pmod(F.col("store_id") * 3, F.lit(4)) + 1).cast("int")))
        .withColumn("opened_year", (F.lit(2012) + F.pmod(F.col("store_id") * 5, F.lit(12))).cast("int"))
        .select("store_id", "region", "store_type", "opened_year")
    )


def generate_dim_promotion(spark: Any, promotions: int):
    from pyspark.sql import functions as F

    promo_types = F.array(*[F.lit(value) for value in ("None", "Coupon", "Holiday", "Clearance", "Loyalty")])

    return (
        spark.range(0, promotions + 1)
        .withColumnRenamed("id", "promotion_id")
        .withColumn("promotion_type", F.element_at(promo_types, (F.pmod(F.col("promotion_id"), F.lit(5)) + 1).cast("int")))
        .withColumn("discount_rate", F.when(F.col("promotion_id") == 0, F.lit(0.0)).otherwise(F.round(F.lit(0.03) + F.rand(21) * 0.27, 2)))
        .select("promotion_id", "promotion_type", "discount_rate")
    )


def generate_fact_sales(spark: Any, config: PocConfig):
    from pyspark.sql import functions as F

    sale_id = F.col("sale_id")
    date_offset = F.pmod(sale_id * 13 + F.floor(F.rand(101) * 730).cast("long"), F.lit(730)).cast("int")
    sale_date = F.date_add(F.to_date(F.lit("2024-01-01")), date_offset)
    product_id = F.when(F.rand(102) < 0.38, F.lit(1)).otherwise(
        F.pmod(sale_id * 17 + F.floor(F.rand(103) * config.products).cast("long"), F.lit(config.products)) + 1
    )
    promotion_id = F.when(F.rand(104) < 0.62, F.lit(0)).otherwise(
        F.pmod(sale_id * 19 + F.floor(F.rand(105) * config.promotions).cast("long"), F.lit(config.promotions)) + 1
    )
    quantity = (F.pmod(sale_id * 7 + F.floor(F.rand(106) * 5).cast("long"), F.lit(5)) + 1).cast("int")
    unit_price = F.round(F.lit(8.0) + F.pmod(product_id * 17, F.lit(190)) + F.rand(107) * 20, 2)
    discount_rate = F.when(promotion_id == 0, F.lit(0.0)).otherwise(F.lit(0.04) + F.rand(108) * 0.24)
    discount_amount = F.round(quantity * unit_price * discount_rate, 2)

    return (
        spark.range(0, config.fact_rows, 1, max(config.shuffle_partitions, 4))
        .withColumnRenamed("id", "sale_id")
        .withColumn("date_id", F.date_format(sale_date, "yyyyMMdd").cast("int"))
        .withColumn("year_month", F.date_format(sale_date, "yyyy-MM"))
        .withColumn("customer_id", (F.pmod(sale_id * 29 + F.floor(F.rand(109) * config.customers).cast("long"), F.lit(config.customers)) + 1).cast("long"))
        .withColumn("product_id", product_id.cast("long"))
        .withColumn("store_id", (F.pmod(sale_id * 31 + F.floor(F.rand(110) * config.stores).cast("long"), F.lit(config.stores)) + 1).cast("long"))
        .withColumn("promotion_id", promotion_id.cast("long"))
        .withColumn("quantity", quantity)
        .withColumn("unit_price", unit_price)
        .withColumn("discount_amount", discount_amount)
        .withColumn("net_amount", F.round(quantity * unit_price - discount_amount, 2))
        .withColumn("returned_flag", F.rand(111) < 0.035)
        .select(
            "sale_id",
            "date_id",
            "year_month",
            "customer_id",
            "product_id",
            "store_id",
            "promotion_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "net_amount",
            "returned_flag",
        )
    )


def write_table(df: Any, path: Path, partition_by: str | None = None) -> None:
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path.as_uri())


def create_external_table(spark: Any, table: str, path: Path) -> None:
    spark.sql(f"CREATE TABLE {DATABASE}.{table} USING PARQUET LOCATION '{path.as_uri()}'")


def generate_warehouse(spark: Any, config: PocConfig) -> dict[str, Any]:
    data_dir = config.base_dir / "data"
    reset_database(spark, data_dir)

    tables = {
        "dim_date": generate_dim_date(spark),
        "dim_product": generate_dim_product(spark, config.products),
        "dim_customer": generate_dim_customer(spark, config.customers),
        "dim_store": generate_dim_store(spark, config.stores),
        "dim_promotion": generate_dim_promotion(spark, config.promotions),
        "fact_sales": generate_fact_sales(spark, config),
    }

    table_paths: dict[str, Path] = {}
    for table_name, df in tables.items():
        path = data_dir / table_name
        table_paths[table_name] = path
        partition_by = "year_month" if table_name == "fact_sales" else None
        write_table(df, path, partition_by=partition_by)
        create_external_table(spark, table_name, path)

    profile = {
        "database": DATABASE,
        "table_paths": {name: str(path) for name, path in table_paths.items()},
        "requested_sizes": {
            "fact_rows": config.fact_rows,
            "products": config.products,
            "customers": config.customers,
            "stores": config.stores,
            "promotions": config.promotions,
        },
        "actual_counts": {},
    }
    for table_name in table_paths:
        profile["actual_counts"][table_name] = spark.table(f"{DATABASE}.{table_name}").count()
    return profile


def analyze_tables(spark: Any) -> None:
    table_columns = {
        "fact_sales": ["date_id", "year_month", "customer_id", "product_id", "store_id", "promotion_id", "returned_flag"],
        "dim_date": ["date_id", "year_month", "fiscal_year"],
        "dim_product": ["product_id", "category", "department", "is_premium"],
        "dim_customer": ["customer_id", "segment", "region"],
        "dim_store": ["store_id", "region", "store_type"],
        "dim_promotion": ["promotion_id", "promotion_type"],
    }
    for table, columns in table_columns.items():
        full_name = f"{DATABASE}.{table}"
        spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS").collect()
        column_sql = ", ".join(columns)
        try:
            spark.sql(f"ANALYZE TABLE {full_name} COMPUTE STATISTICS FOR COLUMNS {column_sql}").collect()
        except Exception as exc:
            print(f"Column statistics failed for {full_name}: {exc}")


def load_queries(query_dir: Path) -> list[tuple[str, str]]:
    queries = []
    for path in sorted(query_dir.glob("*.sql")):
        queries.append((path.stem, path.read_text(encoding="utf-8").strip().rstrip(";")))
    if not queries:
        raise RuntimeError(f"No .sql files found in {query_dir}")
    return queries


def explain_dataframe(df: Any, mode: str) -> str:
    try:
        return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)
    except Exception:
        buffer = StringIO()
        with redirect_stdout(buffer):
            df.explain(mode=mode)
        return buffer.getvalue()


def sql_explain_cost(spark: Any, query_sql: str) -> str:
    try:
        rows = spark.sql(f"EXPLAIN COST {query_sql}").collect()
        return "\n".join(row[0] for row in rows)
    except Exception as exc:
        return f"EXPLAIN COST failed: {exc}"


def save_text(path: Path, content: str) -> None:
    path.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")


def parse_size_to_bytes(value: str, unit: str) -> int | None:
    try:
        numeric = float(value)
    except ValueError:
        return None
    multipliers = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
        "PiB": 1024**5,
        "EiB": 1024**6,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "PB": 1000**5,
        "EB": 1000**6,
    }
    multiplier = multipliers.get(unit)
    if multiplier is None:
        return None
    return int(numeric * multiplier)


def parse_static_indicators(plan_text: str) -> dict[str, Any]:
    sizes = []
    for match in re.finditer(r"sizeInBytes=([0-9.]+)\s*([KMGTPE]?i?B|[KMGTPE]?B)", plan_text):
        parsed = parse_size_to_bytes(match.group(1), match.group(2))
        if parsed is not None:
            sizes.append(parsed)

    row_counts = []
    for match in re.finditer(r"rowCount=([0-9.Ee+-]+)", plan_text):
        try:
            row_counts.append(float(match.group(1)))
        except ValueError:
            continue

    return {
        "exchange_count": len(re.findall(r"\bExchange\b", plan_text)),
        "broadcast_exchange_count": len(re.findall(r"\bBroadcastExchange\b", plan_text)),
        "broadcast_join_count": len(re.findall(r"\bBroadcastHashJoin\b", plan_text)),
        "sort_merge_join_count": len(re.findall(r"\bSortMergeJoin\b", plan_text)),
        "hash_aggregate_count": len(re.findall(r"\bHashAggregate\b", plan_text)),
        "window_count": len(re.findall(r"\bWindow\b", plan_text)),
        "sort_count": len(re.findall(r"\bSort\b", plan_text)),
        "scan_count": len(re.findall(r"\bScan\b", plan_text)),
        "estimated_size_in_bytes_max": max(sizes) if sizes else None,
        "estimated_row_count_max": max(row_counts) if row_counts else None,
    }


def status_tracker_metrics(spark: Any, job_group: str, wall_clock_ms: int) -> dict[str, Any]:
    tracker = spark.sparkContext.statusTracker()
    job_ids = list(tracker.getJobIdsForGroup(job_group))
    stage_ids: set[int] = set()
    task_count = 0

    for job_id in job_ids:
        job_info = tracker.getJobInfo(job_id)
        if not job_info:
            continue
        for stage_id in list(job_info.stageIds):
            stage_ids.add(int(stage_id))

    for stage_id in sorted(stage_ids):
        stage_info = tracker.getStageInfo(stage_id)
        if stage_info:
            task_count += int(stage_info.numTasks)

    return {
        "wall_clock_ms": wall_clock_ms,
        "job_count": len(job_ids),
        "stage_count": len(stage_ids),
        "task_count": task_count,
        "job_ids": sorted(int(job_id) for job_id in job_ids),
        "stage_ids": sorted(stage_ids),
        "shuffle_read_bytes": None,
        "shuffle_write_bytes": None,
        "spill_bytes": None,
        "max_task_duration_ms": None,
        "median_task_duration_ms": None,
    }


def find_event_log_path(event_log_dir: Path, app_id: str) -> Path | None:
    for candidate in (event_log_dir / f"{app_id}.inprogress", event_log_dir / app_id):
        if candidate.exists():
            return candidate
    matches = sorted(event_log_dir.glob(f"{app_id}*"))
    return matches[0] if matches else None


def enrich_runtime_metrics_from_event_log(
    spark: Any,
    runtime_indicators: dict[str, Any],
    event_log_dir: Path,
) -> dict[str, Any]:
    stage_ids = {int(stage_id) for stage_id in runtime_indicators.get("stage_ids", [])}
    if not stage_ids:
        return runtime_indicators

    try:
        spark._jsc.sc().listenerBus().waitUntilEmpty(10000)
    except Exception:
        pass

    app_id = spark.sparkContext.applicationId
    log_path = find_event_log_path(event_log_dir, app_id)
    if log_path is None:
        return runtime_indicators

    task_durations: list[int] = []
    shuffle_read = 0
    shuffle_write = 0
    spill = 0

    try:
        with log_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if '"SparkListenerTaskEnd"' not in line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("Event") != "SparkListenerTaskEnd":
                    continue
                if int(event.get("Stage ID", -1)) not in stage_ids:
                    continue
                metrics = event.get("Task Metrics") or {}
                task_durations.append(int(metrics.get("Executor Run Time", 0)))
                read_metrics = metrics.get("Shuffle Read Metrics") or {}
                shuffle_read += int(read_metrics.get("Remote Bytes Read", 0))
                shuffle_read += int(read_metrics.get("Local Bytes Read", 0))
                write_metrics = metrics.get("Shuffle Write Metrics") or {}
                shuffle_write += int(write_metrics.get("Shuffle Bytes Written", 0))
                spill += int(metrics.get("Memory Bytes Spilled", 0))
                spill += int(metrics.get("Disk Bytes Spilled", 0))
    except OSError:
        return runtime_indicators

    if not task_durations:
        return runtime_indicators

    sorted_durations = sorted(task_durations)
    mid = len(sorted_durations) // 2
    if len(sorted_durations) % 2 == 1:
        median = sorted_durations[mid]
    else:
        median = (sorted_durations[mid - 1] + sorted_durations[mid]) // 2

    return {
        **runtime_indicators,
        "shuffle_read_bytes": shuffle_read,
        "shuffle_write_bytes": shuffle_write,
        "spill_bytes": spill,
        "max_task_duration_ms": max(task_durations),
        "median_task_duration_ms": int(median),
    }


def detect_smj_on_expected_dimensions(plan_text: str, expected_dims: list[str]) -> list[str]:
    hits: list[str] = []
    for match in re.finditer(r"SortMergeJoin", plan_text):
        window = plan_text[match.end(): match.end() + 1500]
        for dim in expected_dims:
            if dim in hits:
                continue
            if re.search(rf"\b{re.escape(dim)}\b", window):
                hits.append(dim)
    return hits


def detect_unfiltered_fact_scans(plan_text: str, fact_tables: list[str]) -> list[str]:
    hits: list[str] = []
    for fact in fact_tables:
        pattern = re.escape(fact)
        for match in re.finditer(rf"Scan\s+parquet\s+{pattern}\b", plan_text):
            window = plan_text[match.end(): match.end() + 1500]
            part_match = re.search(r"PartitionFilters:\s*\[([^\]]*)\]", window)
            pushed_match = re.search(r"PushedFilters:\s*\[([^\]]*)\]", window)
            if not part_match or not pushed_match:
                continue
            if part_match.group(1).strip() or pushed_match.group(1).strip():
                continue
            if fact not in hits:
                hits.append(fact)
    return hits


def load_alert_rules(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_alerts(
    static_indicators: dict[str, Any],
    runtime_indicators: dict[str, Any],
    plan_text: str,
    rules: dict[str, Any],
) -> list[dict[str, Any]]:
    alerts = []

    checks = [
        ("exchange_count", "max_exchange_count", "many_exchanges"),
        ("sort_merge_join_count", "max_sort_merge_join_count", "sort_merge_join_present"),
        ("sort_count", "max_sort_count", "many_sorts"),
    ]
    for metric, rule_name, alert_name in checks:
        value = static_indicators.get(metric)
        threshold = rules.get(rule_name)
        if value is not None and threshold is not None and value > threshold:
            alerts.append(
                {
                    "rule": alert_name,
                    "severity": "warning",
                    "metric": metric,
                    "value": value,
                    "threshold": threshold,
                }
            )

    estimated_size = static_indicators.get("estimated_size_in_bytes_max")
    max_estimated_size = rules.get("max_estimated_size_bytes")
    if estimated_size is not None and max_estimated_size is not None and estimated_size > max_estimated_size:
        alerts.append(
            {
                "rule": "large_estimated_plan_size",
                "severity": "warning",
                "metric": "estimated_size_in_bytes_max",
                "value": estimated_size,
                "threshold": max_estimated_size,
            }
        )

    for metric, rule_name, alert_name in (
        ("wall_clock_ms", "max_wall_clock_ms", "long_wall_clock_time"),
        ("stage_count", "max_stage_count", "many_stages"),
        ("task_count", "max_task_count", "many_tasks"),
        ("shuffle_read_bytes", "max_shuffle_read_bytes", "high_shuffle_read"),
        ("shuffle_write_bytes", "max_shuffle_write_bytes", "high_shuffle_write"),
    ):
        value = runtime_indicators.get(metric)
        threshold = rules.get(rule_name)
        if value is not None and threshold is not None and value > threshold:
            alerts.append(
                {
                    "rule": alert_name,
                    "severity": "warning",
                    "metric": metric,
                    "value": value,
                    "threshold": threshold,
                }
            )

    spill_bytes = runtime_indicators.get("spill_bytes")
    spill_threshold = rules.get("warn_on_spill_bytes_gt")
    if spill_bytes is not None and spill_threshold is not None and spill_bytes > spill_threshold:
        alerts.append(
            {
                "rule": "spill_detected",
                "severity": "warning",
                "metric": "spill_bytes",
                "value": spill_bytes,
                "threshold": spill_threshold,
            }
        )

    max_dur = runtime_indicators.get("max_task_duration_ms")
    median_dur = runtime_indicators.get("median_task_duration_ms")
    skew_threshold = rules.get("max_task_skew_ratio")
    if max_dur is not None and median_dur and skew_threshold is not None:
        ratio = max_dur / median_dur
        if ratio > skew_threshold:
            alerts.append(
                {
                    "rule": "task_skew_detected",
                    "severity": "warning",
                    "metric": "max_to_median_task_duration_ratio",
                    "value": round(ratio, 2),
                    "threshold": skew_threshold,
                }
            )

    expected_dims = rules.get("expected_broadcast_dimensions") or []
    if expected_dims and static_indicators.get("sort_merge_join_count", 0) > 0:
        offenders = detect_smj_on_expected_dimensions(plan_text, expected_dims)
        if offenders:
            alerts.append(
                {
                    "rule": "smj_on_expected_broadcast_dimension",
                    "severity": "warning",
                    "metric": "dimensions",
                    "value": offenders,
                    "threshold": expected_dims,
                }
            )

    fact_tables = rules.get("large_fact_tables") or []
    if fact_tables:
        offenders = detect_unfiltered_fact_scans(plan_text, fact_tables)
        if offenders:
            alerts.append(
                {
                    "rule": "unfiltered_fact_scan",
                    "severity": "warning",
                    "metric": "tables",
                    "value": offenders,
                    "threshold": fact_tables,
                }
            )

    return alerts


def selected_spark_conf(spark: Any) -> dict[str, str | None]:
    keys = [
        "spark.master",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.shuffle.partitions",
        "spark.sql.cbo.enabled",
        "spark.sql.statistics.histogram.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
        "spark.sql.warehouse.dir",
    ]
    spark_context_conf = spark.sparkContext.getConf()
    values: dict[str, str | None] = {}
    for key in keys:
        try:
            values[key] = spark.conf.get(key)
        except Exception:
            values[key] = spark_context_conf.get(key, None)
    return values


def table_names_from_sql(query_sql: str) -> list[str]:
    tables = sorted(set(re.findall(rf"{DATABASE}\.([A-Za-z0-9_]+)", query_sql)))
    return [f"{DATABASE}.{table}" for table in tables]


def run_query_capture(
    spark: Any,
    query_id: str,
    query_sql: str,
    run_dir: Path,
    config: PocConfig,
    alert_rules: dict[str, Any],
) -> dict[str, Any]:
    query_dir = run_dir / query_id
    result_dir = query_dir / "result_parquet"
    query_dir.mkdir(parents=True, exist_ok=True)
    save_text(query_dir / "query.sql", query_sql)

    df = spark.sql(query_sql)
    explain_outputs = {
        "simple": explain_dataframe(df, "simple"),
        "extended": explain_dataframe(df, "extended"),
        "cost": explain_dataframe(df, "cost"),
        "formatted": explain_dataframe(df, "formatted"),
    }
    for mode, output in explain_outputs.items():
        save_text(query_dir / f"explain_{mode}.txt", output)
    save_text(query_dir / "sql_explain_cost.txt", sql_explain_cost(spark, query_sql))

    job_group = f"{config.run_id}_{query_id}"
    spark.sparkContext.setJobGroup(job_group, f"Run {query_id}", interruptOnCancel=True)
    started = time.perf_counter()
    df.write.mode("overwrite").format("parquet").save(result_dir.as_uri())
    wall_clock_ms = int((time.perf_counter() - started) * 1000)
    runtime_indicators = status_tracker_metrics(spark, job_group, wall_clock_ms)
    runtime_indicators = enrich_runtime_metrics_from_event_log(
        spark, runtime_indicators, config.base_dir / "spark-events"
    )
    spark.sparkContext._jsc.clearJobGroup()

    executed_plan = ""
    try:
        executed_plan = df._jdf.queryExecution().executedPlan().toString()
    except Exception as exc:
        executed_plan = f"Executed plan capture failed: {exc}"
    save_text(query_dir / "executed_plan_after_action.txt", executed_plan)

    combined_plan_text = "\n".join([*explain_outputs.values(), executed_plan])
    static_indicators = parse_static_indicators(combined_plan_text)
    alerts = build_alerts(static_indicators, runtime_indicators, combined_plan_text, alert_rules)

    metadata = {
        "run_id": config.run_id,
        "environment": "colab-standalone" if config.start_standalone else "spark",
        "spark_version": spark.version,
        "query_id": query_id,
        "query_sql": query_sql,
        "tables": table_names_from_sql(query_sql),
        "result_path": str(result_dir),
        "spark_conf": selected_spark_conf(spark),
        "plan_files": {
            "simple": "explain_simple.txt",
            "extended": "explain_extended.txt",
            "cost": "explain_cost.txt",
            "formatted": "explain_formatted.txt",
            "sql_explain_cost": "sql_explain_cost.txt",
            "executed_plan_after_action": "executed_plan_after_action.txt",
        },
        "static_indicators": static_indicators,
        "runtime_indicators": runtime_indicators,
        "alerts": alerts,
    }
    (query_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return metadata


def run_query_capture_with_conf(
    spark: Any,
    query_id: str,
    query_sql: str,
    run_dir: Path,
    config: PocConfig,
    alert_rules: dict[str, Any],
    conf_overrides: dict[str, str],
) -> dict[str, Any]:
    previous_values: dict[str, str | None] = {}
    for key, value in conf_overrides.items():
        try:
            previous_values[key] = spark.conf.get(key)
        except Exception:
            previous_values[key] = None
        spark.conf.set(key, value)

    try:
        metadata = run_query_capture(spark, query_id, query_sql, run_dir, config, alert_rules)
        metadata["spark_conf_overrides"] = conf_overrides
        query_dir = run_dir / query_id
        (query_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        return metadata
    finally:
        for key, previous_value in previous_values.items():
            if previous_value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, previous_value)


def write_run_files(
    spark: Any,
    config: PocConfig,
    run_dir: Path,
    data_profile: dict[str, Any],
    query_results: list[dict[str, Any]],
) -> None:
    environment = {
        "run_id": config.run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "python_version": sys.version,
        "base_dir": str(config.base_dir),
        "query_dir": str(config.query_dir),
        "master": config.master,
        "spark_conf": selected_spark_conf(spark),
    }
    (run_dir / "environment.json").write_text(json.dumps(environment, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "data_profile.json").write_text(json.dumps(data_profile, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "run_summary.json").write_text(json.dumps(query_results, indent=2, sort_keys=True), encoding="utf-8")

    summary_path = run_dir / "run_summary.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "query_id",
                "wall_clock_ms",
                "job_count",
                "stage_count",
                "task_count",
                "exchange_count",
                "broadcast_join_count",
                "sort_merge_join_count",
                "window_count",
                "estimated_size_in_bytes_max",
                "estimated_row_count_max",
                "alert_count",
            ],
        )
        writer.writeheader()
        for result in query_results:
            static = result["static_indicators"]
            runtime = result["runtime_indicators"]
            writer.writerow(
                {
                    "query_id": result["query_id"],
                    "wall_clock_ms": runtime.get("wall_clock_ms"),
                    "job_count": runtime.get("job_count"),
                    "stage_count": runtime.get("stage_count"),
                    "task_count": runtime.get("task_count"),
                    "exchange_count": static.get("exchange_count"),
                    "broadcast_join_count": static.get("broadcast_join_count"),
                    "sort_merge_join_count": static.get("sort_merge_join_count"),
                    "window_count": static.get("window_count"),
                    "estimated_size_in_bytes_max": static.get("estimated_size_in_bytes_max"),
                    "estimated_row_count_max": static.get("estimated_row_count_max"),
                    "alert_count": len(result.get("alerts", [])),
                }
            )


def parse_args() -> PocConfig:
    parser = argparse.ArgumentParser(description="Run the Colab Spark SQL plan-analysis POC.")
    parser.add_argument("--base-dir", type=Path, default=Path("/content/spark_sql_plan_poc"))
    parser.add_argument("--query-dir", type=Path, default=DEFAULT_QUERY_DIR)
    parser.add_argument("--alert-rules", type=Path, default=DEFAULT_ALERT_RULES)
    parser.add_argument("--master", default="spark://localhost:7077")
    parser.add_argument("--fact-rows", type=int, default=300_000)
    parser.add_argument("--products", type=int, default=2_000)
    parser.add_argument("--customers", type=int, default=25_000)
    parser.add_argument("--stores", type=int, default=120)
    parser.add_argument("--promotions", type=int, default=24)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--start-standalone", action="store_true")
    parser.add_argument("--stop-standalone-after-run", action="store_true")
    parser.add_argument("--skip-analyze", action="store_true")
    args = parser.parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_colab_poc")
    return PocConfig(
        base_dir=args.base_dir.expanduser().resolve(),
        query_dir=args.query_dir.expanduser().resolve(),
        alert_rules_path=args.alert_rules.expanduser().resolve(),
        master=args.master,
        fact_rows=args.fact_rows,
        products=args.products,
        customers=args.customers,
        stores=args.stores,
        promotions=args.promotions,
        shuffle_partitions=args.shuffle_partitions,
        run_id=run_id,
        start_standalone=args.start_standalone,
        stop_standalone_after_run=args.stop_standalone_after_run,
        skip_analyze=args.skip_analyze,
    )


def main() -> None:
    config = parse_args()
    config.base_dir.mkdir(parents=True, exist_ok=True)

    ensure_pyspark_importable()
    if config.start_standalone:
        start_standalone_cluster(config.base_dir)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    run_dir = config.base_dir / "artifacts" / "runs" / config.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating warehouse in {config.base_dir}")
    data_profile = generate_warehouse(spark, config)

    alert_rules = load_alert_rules(config.alert_rules_path)
    queries = load_queries(config.query_dir)
    query_results: list[dict[str, Any]] = []

    if queries:
        first_query_id, first_query_sql = queries[0]
        before_stats_id = f"{first_query_id}_before_stats"
        print(f"Capturing before-statistics plan for {before_stats_id}")
        query_results.append(run_query_capture(spark, before_stats_id, first_query_sql, run_dir, config, alert_rules))

    if not config.skip_analyze:
        print("Computing table and column statistics")
        analyze_tables(spark)

    for query_id, query_sql in queries:
        variants = QUERY_CONF_VARIANTS.get(query_id)
        if variants:
            for variant_name, conf_overrides in variants:
                variant_query_id = f"{query_id}_{variant_name}"
                print(f"Running {variant_query_id}")
                query_results.append(
                    run_query_capture_with_conf(
                        spark,
                        variant_query_id,
                        query_sql,
                        run_dir,
                        config,
                        alert_rules,
                        conf_overrides,
                    )
                )
        else:
            print(f"Running {query_id}")
            query_results.append(run_query_capture(spark, query_id, query_sql, run_dir, config, alert_rules))

    write_run_files(spark, config, run_dir, data_profile, query_results)

    print(f"Run complete: {run_dir}")
    print(f"Summary: {run_dir / 'run_summary.csv'}")
    spark.stop()
    if config.stop_standalone_after_run:
        stop_standalone_cluster(config.base_dir)


if __name__ == "__main__":
    main()
