# Manual Plan Interpretations

This folder holds **hand-produced** plan interpretations: the output an LLM
*would* generate from a Workstream 4 `llm_package.json`, written manually
before the automated LLM caller exists.

Status: **draft — kept for later revision.** These files are not produced by
a verified LLM caller. When the caller pass lands (see the parent
`README.md`, "Later passes"), regenerate the outputs and diff them against
these drafts.

## Why this folder exists

The current Workstream 4 pass implements the assembler only. There is no code
that takes `llm_package.json` + `prompts/plan_interpreter_prompt.md` and emits
`llm_output.json`. To exercise the contract end-to-end before that code
exists, the interpretation was done by hand — a human/assistant playing the
role of the model described in the prompt.

## Workflow used

1. **Assemble the packages (deterministic, offline).**
   Run the Workstream 4 assembler against a Workstream 2 run:
   ```bash
   python workstream_04_llm_plan_interpreter/assemble_package.py \
     --run-dir workstream_02_colab_poc/artifacts/runs/<run-id>
   ```
   This writes one `llm_package.json` per query subdirectory plus a run-level
   `llm_packages_index.json`. No Spark re-run is needed.

2. **Read the target query's `llm_package.json`.**
   That single JSON is the only input the interpreter prompt expects. Its
   fields are listed in the prompt's "Input" section.

3. **Interpret it against `prompts/plan_interpreter_prompt.md`.**
   Treat the prompt as the model's instructions and the package as the model
   input. Produce the single JSON object the prompt's "Output contract"
   mandates: `summary`, `expensive_parts[]`, `suggestions[]`, `warnings[]`,
   `confidence`. Follow the prompt's grounding rules — every claim tied to a
   `plan_excerpts` snippet, an `indicator_summary` field, a `runtime_metrics`
   field, or an `alerts` entry; null/missing metrics flagged under `warnings`;
   `confidence` downgraded when stats are missing.

4. **Save the output here, one JSON file per query.**
   File naming mirrors what the future caller will write inside the run tree
   (`<query_id>.llm_output.json`), but kept in this separate folder so the run
   artifacts stay untouched and the manual drafts remain comparable to the
   eventual automated output.

## Layout

```text
manual_interpretations/
├── README.md                       <- this file
└── 20260521T034715Z_colab_poc/      <- one folder per Workstream 2 run
    ├── q001_monthly_category_revenue.llm_output.json
    ├── q001_monthly_category_revenue_before_stats.llm_output.json
    └── q001_contrast_notes.md       <- contrast + caveats to revisit
```

## Open items for revision

These were noticed while interpreting and should be checked against the
assembler / Workstream 2 (they affect any future automated interpretation):

- `indicator_summary` counts (`exchange_count`, `broadcast_join_count`,
  `sort_count`) appear inflated — they look summed across the
  formatted/executed/cost excerpts rather than counted once per query.
- `fact_sales` reports `rowCount=0` in the with-stats `EXPLAIN COST`, which
  conflicts with the query producing a result.
- `shuffle_read_bytes` / `shuffle_write_bytes` are `0` even when shuffle
  Exchanges are present — confirm whether the metric is captured.

See `20260521T034715Z_colab_poc/q001_contrast_notes.md` for detail.
