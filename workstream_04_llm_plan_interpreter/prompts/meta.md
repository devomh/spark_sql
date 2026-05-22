# Subagent Metaprompts for the Plan Prompts

The two prompt templates in this folder — `plan_interpreter_prompt.md` and
`plan_explainer_prompt.md` — *describe* how a model should behave, but they
are not themselves runnable. The automated LLM caller that will consume them
(`llm_package.json` + template → `llm_output.json`) does not exist yet.

Until it does, the templates are exercised by hand: a **metaprompt** wraps a
template and is handed to a subagent. The metaprompt tells the subagent which
template to treat as its instructions, which `llm_package.json` file(s) to
consume as input, and how to return the result. This file records the
reusable metaprompt and the run data it points at, so the same checks can be
reproduced.

## Paths in this file

All paths below are written relative to the repository root, as
`<REPO_ROOT>/...`. This keeps the file correct if the repo is cloned or moved
— only the value of `<REPO_ROOT>` changes.

Before a metaprompt is handed to a subagent, **substitute `<REPO_ROOT>` with
the absolute repository path.** The `Read` tool requires absolute paths, and a
subagent's working directory is not guaranteed, so the substitution must
produce absolute paths — do not leave them relative.

- On the current machine: `<REPO_ROOT>` = `/mnt/6BC34F5A663C63E2/Projects_2026/spark_sql`
- Portably: `<REPO_ROOT>` = the output of `git rev-parse --show-toplevel` run inside the repo.

## Metaprompt template (fill in the placeholders)

```text
You are role-playing as the LLM described by a prompt template. Do the following:

1. Read the prompt template at:
   <REPO_ROOT>/workstream_04_llm_plan_interpreter/prompts/{TEMPLATE_FILE}

2. Treat that file as your full system instructions.

3. Read the input package(s) for query {QUERY_ID} from this run:
   <REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/{RUN_ID}/{QUERY_DIR_1}/llm_package.json
   <REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/{RUN_ID}/{QUERY_DIR_2}/llm_package.json   <- omit if the query has a single capture
   {MULTI_PACKAGE_NOTE}   <- omit if only one package

4. Following the prompt template exactly, produce the {OUTPUT_KIND} for {QUERY_ID}.

Return ONLY the final {OUTPUT_FORMAT} as your response — do not write any files,
do not add commentary about your process. Read ONLY the files listed above; do
not read any other files in the repository (in particular, do not look for any
existing answer, notes file, or other prompt template). Produce the output
solely from the prompt template and the package file(s).
```

Placeholder values per template:

| Placeholder | `plan_explainer_prompt.md` | `plan_interpreter_prompt.md` |
|---|---|---|
| `{TEMPLATE_FILE}` | `plan_explainer_prompt.md` | `plan_interpreter_prompt.md` |
| `{OUTPUT_KIND}` | `plain-language plan explanation` | `JSON plan interpretation` |
| `{OUTPUT_FORMAT}` | `Markdown explanation` | `JSON object` |

`{MULTI_PACKAGE_NOTE}` is used only when a query has more than one capture
(e.g. q001 has a with-stats and a before-stats package). Example text:
"These are the same query captured with and without table statistics. Produce
ONE result, drawing plan/scan details from whichever excerpt shows them."

## Metaprompt A — `plan_explainer_prompt.md` (as run for q001)

This is the metaprompt used to verify the explainer template; it was run
twice (once before and once after the template was made self-contained).
Substitute `<REPO_ROOT>` before dispatching.

```text
You are role-playing as the LLM described by a prompt template. Do the following:

1. Read the prompt template at:
   <REPO_ROOT>/workstream_04_llm_plan_interpreter/prompts/plan_explainer_prompt.md

2. Treat that file as your full system instructions.

3. Read the input package(s) for query q001 from this run:
   <REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/20260521T034715Z_colab_poc/q001_monthly_category_revenue/llm_package.json
   <REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/20260521T034715Z_colab_poc/q001_monthly_category_revenue_before_stats/llm_package.json
   These are the same query q001 captured with and without table statistics. Produce ONE explanation for q001, drawing plan/scan details from whichever excerpt shows them.

4. Following the prompt template exactly, produce the plain-language plan explanation for q001.

Return ONLY the final Markdown explanation as your response — do not write any files, do not add commentary about your process. Read ONLY the three files listed above; do not read any other files in the repository (in particular, do not look for any existing answer, notes file, or other prompt template). Produce the explanation solely from the prompt template and the two package files.
```

## Metaprompt B — `plan_interpreter_prompt.md` (single-capture example, q002)

Same shape, pointed at the interpreter template and a single-package query.
Substitute `<REPO_ROOT>` before dispatching.

```text
You are role-playing as the LLM described by a prompt template. Do the following:

1. Read the prompt template at:
   <REPO_ROOT>/workstream_04_llm_plan_interpreter/prompts/plan_interpreter_prompt.md

2. Treat that file as your full system instructions.

3. Read the input package for query q002 from this run:
   <REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/20260521T034715Z_colab_poc/q002_customer_window_revenue/llm_package.json

4. Following the prompt template exactly, produce the JSON plan interpretation for q002.

Return ONLY the final JSON object as your response — do not write any files, do not add commentary about your process. Read ONLY the two files listed above; do not read any other files in the repository (in particular, do not look for any existing answer, notes file, or other prompt template). Produce the output solely from the prompt template and the package file.
```

## Run data

Run id (`{RUN_ID}`): `20260521T034715Z_colab_poc`

Run directory:
`<REPO_ROOT>/workstream_02_colab_poc/artifacts/runs/20260521T034715Z_colab_poc`

Each query subdirectory holds an `llm_package.json` (written by
`assemble_package.py`). The package input path is
`<run-dir>/<query-dir>/llm_package.json`.

| Query | Query subdirectory (`{QUERY_DIR}`) | Captures |
|---|---|---|
| q001 | `q001_monthly_category_revenue` | with stats |
| q001 | `q001_monthly_category_revenue_before_stats` | before stats — pair with the above |
| q002 | `q002_customer_window_revenue` | single |
| q003 | `q003_grouping_sets_profitability` | single |
| q004 | `q004_broadcast_star_join_auto_broadcast` | auto-broadcast |
| q004 | `q004_broadcast_star_join_no_broadcast` | broadcast disabled — pair with the above |
| q005 | `q005_shuffle_partition_sensitivity_shuffle_16` | 16 shuffle partitions |
| q005 | `q005_shuffle_partition_sensitivity_shuffle_4` | 4 shuffle partitions — pair with the above |
| q006 | `q006_skewed_product_hotspots` | single |

If the assembler is re-run for a different run, only `{RUN_ID}` (and thus the
run directory) changes; the subdirectory names and the `llm_package.json`
filename are stable.

## Notes

- The subagent is a **read-only verification harness**. It returns the
  generated output as its message; it does not write files. Save the result
  yourself into `<REPO_ROOT>/workstream_04_llm_plan_interpreter/manual_interpretations/<RUN_ID>/`
  using the naming in that folder's `README.md` (`<query_id>.llm_output.json`
  for the interpreter, `<query_id>_plan_explanation.md` for the explainer).
- The "read ONLY the files listed" instruction matters: it keeps the
  verification clean (the subagent must not peek at an existing manual answer)
  and confirms each template is self-contained — neither template should need
  the subagent to open any other file.
- Pair the two captures of q001, q004 and q005 in a single metaprompt when a
  with/without contrast is wanted; otherwise run each capture on its own.
