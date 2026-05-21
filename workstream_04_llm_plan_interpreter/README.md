# Workstream 4: LLM-Ready Plan Interpretation

This workstream prepares the artifacts captured by Workstream 2 (or any producer that follows the same layout) so an LLM can later explain Spark SQL plans and suggest tuning experiments.

This pass implements the **assembler only**. It produces a deterministic, offline `llm_package.json` per query. The actual LLM caller is intentionally deferred to a later pass; the prompt template that the caller will use lives at `prompts/plan_interpreter_prompt.md` and codifies the LLM output contract.

## What the assembler does

For each query directory under a Workstream 2 run, the assembler reads:

- `query.sql`, `metadata.json`, `explain_formatted.txt`, `executed_plan_after_action.txt`, `explain_cost.txt`
- Run-level `environment.json` and `data_profile.json`
- A static `table_descriptions.json` (short hand-written notes per table)

It then writes one `llm_package.json` per query with these fields:

- `package_version`, `generated_at_utc`
- `run_id`, `query_id`, `environment`, `spark_version`, `databricks_runtime`
- `spark_conf` (filtered) and `spark_conf_overrides` (if the query ran with overrides)
- `query_sql`
- `tables`: list of `{name, description, row_count, size_bytes, path}` rows. Sizes come from summing `*.parquet` files under each table path.
- `indicator_summary`: passed through from `metadata.static_indicators`
- `runtime_metrics`: filtered subset of `metadata.runtime_indicators` (drops noisy job_ids/stage_ids; keeps wall_clock, shuffle, spill, max/median task duration, etc.)
- `alerts`: passed through from `metadata.alerts`
- `plan_excerpts.formatted`, `plan_excerpts.executed`, `plan_excerpts.cost`: trimmed plan text (per-excerpt char cap, `file:/...` paths masked, `InMemoryFileIndex` location blocks collapsed)
- `trimming_notes`: list of transformations applied so the LLM sees what was changed

A run-level `llm_packages_index.json` is also written.

## Usage

```bash
python workstream_04_llm_plan_interpreter/assemble_package.py \
  --run-dir /content/spark_sql_plan_poc/artifacts/runs/<run-id>
```

By default, `llm_package.json` is written into each query subdirectory next to `metadata.json` (in place). To write packages to a separate tree instead:

```bash
python workstream_04_llm_plan_interpreter/assemble_package.py \
  --run-dir /content/spark_sql_plan_poc/artifacts/runs/<run-id> \
  --output-dir /content/llm_packages/<run-id>
```

Other flags:

- `--table-descriptions <path>`: override the static descriptions file (default: `table_descriptions.json` in this folder).
- `--max-plan-chars <int>`: per-excerpt character cap (default 4000).

## Why offline first

The plan calls for "non-LLM first": deterministic extraction (already done in Workstream 2) plus a clean, self-contained package the LLM consumes. Doing the assembler offline:

- Lets us version the input format independently of any LLM provider.
- Makes the pipeline replayable: regenerate packages without re-running Spark.
- Keeps the comparison harness (Milestone 8) tractable — deterministic alerts and LLM output can be diffed file-to-file.

## Later passes

- Add a Claude caller that consumes `llm_package.json` + `prompts/plan_interpreter_prompt.md` and writes `llm_output.json` per query.
- JSON-schema-validate the LLM output against the contract in the prompt.
- Build a comparison harness that flags disagreements between deterministic alerts and the LLM's `expensive_parts` / `suggestions`.

## Layout

```text
workstream_04_llm_plan_interpreter/
├── README.md
├── assemble_package.py
├── table_descriptions.json
└── prompts/
    └── plan_interpreter_prompt.md
```
