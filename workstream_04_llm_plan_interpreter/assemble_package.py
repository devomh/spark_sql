#!/usr/bin/env python3
"""Assemble LLM-ready input packages from a Workstream 2 artifact run.

Reads a run directory produced by `workstream_02_colab_poc/colab_poc.py`
(or any other producer that follows the same layout) and writes one
`llm_package.json` per query, matching the LLM Input Package contract in
`docs/spark_sql_plan.md` (Workstream 4).
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_TABLE_DESCRIPTIONS = ROOT / "table_descriptions.json"
DEFAULT_PROMPT = ROOT / "prompts" / "plan_interpreter_prompt.md"

PLAN_FILES = {
    "formatted": "explain_formatted.txt",
    "executed": "executed_plan_after_action.txt",
    "cost": "explain_cost.txt",
}

RUNTIME_KEEP = (
    "wall_clock_ms",
    "job_count",
    "stage_count",
    "task_count",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "spill_bytes",
    "max_task_duration_ms",
    "median_task_duration_ms",
)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def short_table_name(qualified: str, database: str) -> str:
    if database and qualified.startswith(f"{database}."):
        return qualified[len(database) + 1 :]
    return qualified.split(".", 1)[-1]


def parquet_size_bytes(path: Path) -> int | None:
    if not path.exists():
        return None
    total = 0
    found = False
    for entry in path.rglob("*"):
        if entry.is_file() and entry.suffix == ".parquet":
            total += entry.stat().st_size
            found = True
    return total if found else None


def trim_plan_text(text: str, max_chars: int) -> tuple[str, list[str]]:
    notes: list[str] = []
    if not text:
        return "", notes

    cleaned, count = re.subn(
        r"Location:\s*InMemoryFileIndex\(\d+\s+paths\)\s*\[[^\]]+\]",
        "Location: <file index>",
        text,
    )
    if count:
        notes.append(f"Masked {count} InMemoryFileIndex path block(s)")

    cleaned, count = re.subn(r"file:/[^\s\]\)\,]+", "<file>", cleaned)
    if count:
        notes.append(f"Masked {count} file:/... URI(s)")

    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars].rstrip() + "\n... [truncated]"
        notes.append(f"Truncated to {max_chars} chars")
    return cleaned, notes


def build_table_entries(
    table_names: list[str],
    data_profile: dict[str, Any],
    table_descriptions: dict[str, str],
    run_base: Path | None,
) -> list[dict[str, Any]]:
    database = data_profile.get("database", "")
    actual_counts = data_profile.get("actual_counts", {}) or {}
    table_paths = data_profile.get("table_paths", {}) or {}

    entries: list[dict[str, Any]] = []
    for qualified in table_names:
        short = short_table_name(qualified, database)
        path_str = table_paths.get(short)
        path_obj = Path(path_str) if path_str else None
        size_bytes: int | None = None
        if path_obj is not None:
            size_bytes = parquet_size_bytes(path_obj)
            if size_bytes is None and run_base is not None:
                # Fallback: maybe the run was relocated; try resolving relative
                candidate = run_base / path_obj.name
                size_bytes = parquet_size_bytes(candidate)
        entries.append(
            {
                "name": qualified,
                "description": table_descriptions.get(qualified),
                "row_count": actual_counts.get(short),
                "size_bytes": size_bytes,
                "path": path_str,
            }
        )
    return entries


def select_runtime_metrics(runtime_indicators: dict[str, Any]) -> dict[str, Any]:
    return {key: runtime_indicators.get(key) for key in RUNTIME_KEEP}


def select_spark_conf(spark_conf: dict[str, Any] | None) -> dict[str, Any]:
    if not spark_conf:
        return {}
    keys = (
        "spark.master",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.shuffle.partitions",
        "spark.sql.cbo.enabled",
        "spark.sql.statistics.histogram.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
    )
    return {key: spark_conf.get(key) for key in keys if key in spark_conf}


def assemble_package(
    query_dir: Path,
    environment: dict[str, Any],
    data_profile: dict[str, Any],
    table_descriptions: dict[str, str],
    max_plan_chars: int,
    run_base: Path | None,
) -> dict[str, Any]:
    metadata = load_json(query_dir / "metadata.json")

    excerpts: dict[str, str] = {}
    trimming_notes: list[str] = []
    for label, filename in PLAN_FILES.items():
        plan_path = query_dir / filename
        text = plan_path.read_text(encoding="utf-8") if plan_path.exists() else ""
        trimmed, notes = trim_plan_text(text, max_plan_chars)
        excerpts[label] = trimmed
        trimming_notes.extend(f"{label}: {note}" for note in notes)

    package = {
        "package_version": "1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": metadata.get("run_id"),
        "query_id": metadata.get("query_id"),
        "environment": metadata.get("environment"),
        "spark_version": metadata.get("spark_version") or environment.get("spark_version"),
        "databricks_runtime": environment.get("databricks_runtime")
        or metadata.get("databricks_runtime"),
        "spark_conf": select_spark_conf(metadata.get("spark_conf") or environment.get("spark_conf")),
        "spark_conf_overrides": metadata.get("spark_conf_overrides"),
        "query_sql": metadata.get("query_sql", ""),
        "tables": build_table_entries(
            metadata.get("tables", []) or [],
            data_profile,
            table_descriptions,
            run_base,
        ),
        "indicator_summary": metadata.get("static_indicators", {}) or {},
        "runtime_metrics": select_runtime_metrics(metadata.get("runtime_indicators", {}) or {}),
        "alerts": metadata.get("alerts", []) or [],
        "plan_excerpts": excerpts,
        "trimming_notes": trimming_notes,
    }
    return package


def find_query_dirs(run_dir: Path) -> list[Path]:
    return sorted(
        d for d in run_dir.iterdir() if d.is_dir() and (d / "metadata.json").exists()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble LLM-ready packages from a Workstream 2 artifact run."
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Path to artifacts/runs/<run-id>/ from a Workstream 2 execution.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="If set, write packages under <output-dir>/<query-id>/llm_package.json. "
        "Default: write llm_package.json next to each query's metadata.json (in place).",
    )
    parser.add_argument(
        "--table-descriptions",
        type=Path,
        default=DEFAULT_TABLE_DESCRIPTIONS,
    )
    parser.add_argument(
        "--max-plan-chars",
        type=int,
        default=4000,
        help="Per-excerpt character cap (formatted, executed, cost).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_dir: Path = args.run_dir.expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")

    environment = load_json(run_dir / "environment.json")
    data_profile = load_json(run_dir / "data_profile.json")
    table_descriptions = load_json(args.table_descriptions.expanduser().resolve())

    query_dirs = find_query_dirs(run_dir)
    if not query_dirs:
        raise SystemExit(f"No query directories with metadata.json under {run_dir}")

    index: list[dict[str, Any]] = []
    for query_dir in query_dirs:
        package = assemble_package(
            query_dir,
            environment,
            data_profile,
            table_descriptions,
            args.max_plan_chars,
            run_base=run_dir.parent.parent,
        )
        if args.output_dir is not None:
            out_dir = args.output_dir.expanduser().resolve() / query_dir.name
        else:
            out_dir = query_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "llm_package.json"
        out_path.write_text(
            json.dumps(package, indent=2, sort_keys=True), encoding="utf-8"
        )
        index.append(
            {
                "query_id": package["query_id"],
                "package_path": str(out_path),
                "alert_count": len(package["alerts"]),
                "exchange_count": package["indicator_summary"].get("exchange_count"),
                "wall_clock_ms": package["runtime_metrics"].get("wall_clock_ms"),
            }
        )
        print(f"Wrote {out_path}")

    index_root = args.output_dir.expanduser().resolve() if args.output_dir else run_dir
    index_root.mkdir(parents=True, exist_ok=True)
    index_path = index_root / "llm_packages_index.json"
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {index_path}")


if __name__ == "__main__":
    main()
