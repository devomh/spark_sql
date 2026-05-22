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

3. **Interpret it against a prompt template.** The `prompts/` folder holds
   four templates (see `../prompts/meta.md`):
   - `plan_interpreter_prompt.md` — a JSON tuning review of the package.
   - `plan_explainer_prompt.md` — a plain-language explanation of the package.
   - `cost_interpreter_prompt.md` — a JSON read of a single `EXPLAIN COST`
     output (`explain_cost.txt`), not the package.
   - `cost_explainer_prompt.md` — a plain-language version of the same.
   Treat the chosen template as the model's instructions and its input file
   (the package, or an `explain_cost.txt`) as the model input, and follow the
   template's grounding rules.

4. **Save the output here, one file per query per prompt.**
   `plan_interpreter_prompt.md` output is named `<query_id>.llm_output.json`,
   mirroring what the future caller will write inside the run tree;
   `plan_explainer_prompt.md` output is named `<query_id>_plan_explanation.md`.
   The cost prompts are named `<query_id>_cost_interpretation.json` and
   `<query_id>_cost_explanation.md`. All are kept in this separate folder so
   the run artifacts stay untouched and the manual drafts remain comparable to
   the eventual automated output. A subagent verification run of a template
   (dispatched via the metaprompts in `../prompts/meta.md`) is saved with a
   `.subagent.` infix — e.g. `<query_id>.subagent.llm_output.json` or
   `<query_id>_cost_interpretation.subagent.json` — so it sits beside the
   hand-written draft for comparison without overwriting it.

## Layout

```text
manual_interpretations/
├── README.md                       <- this file
└── 20260521T034715Z_colab_poc/      <- one folder per Workstream 2 run
    ├── q001_monthly_category_revenue.llm_output.json              <- plan_interpreter_prompt.md (with stats)
    ├── q001_monthly_category_revenue_before_stats.llm_output.json <- plan_interpreter_prompt.md (before stats)
    ├── q001_contrast_notes.md       <- contrast + caveats to revisit
    ├── q001_plan_explanation.md     <- plan_explainer_prompt.md (plain-language)
    ├── q002_customer_window_revenue.llm_output.json               <- plan_interpreter_prompt.md
    ├── q002_customer_window_revenue.subagent.llm_output.json       <- plan_interpreter_prompt.md, subagent verification run (Metaprompt B)
    ├── q002_plan_explanation.md     <- plan_explainer_prompt.md (plain-language)
    ├── q001_cost_interpretation.subagent.json                      <- cost_interpreter_prompt.md, subagent verification run (Metaprompt C)
    └── q001_cost_explanation.subagent.md                           <- cost_explainer_prompt.md, subagent verification run (Metaprompt C)
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
