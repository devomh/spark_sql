# q001 — interpretation notes (run 20260521T034715Z_colab_poc)

Draft notes accompanying the two `q001_*.llm_output.json` files in this
folder. Kept for later revision once the automated LLM caller exists.

## What q001 does

A monthly category/region revenue rollup. `fact_sales` is joined to three
dimensions (`dim_date`, `dim_product`, `dim_store`), filtered to a 12-month
window (`year_month BETWEEN '2024-04' AND '2025-03'`) and non-returned sales,
then grouped by `(year_month, category, region)` with a
`COUNT(DISTINCT customer_id)` and ordered by `year_month, net_revenue DESC`.

The SQL and the physical join strategy are **identical** between the two
variants — all three dimensions are broadcast in both. The only real
difference is whether table statistics existed when the plan was captured.

## With stats vs. before stats

| Field | `q001_monthly_category_revenue` | `q001_..._before_stats` |
|---|---|---|
| `wall_clock_ms` | 402 | 5543 (~13.8x) |
| `job_count` / `stage_count` / `task_count` | 2 / 1 / 1 | 5 / 6 / 9 |
| `max_task_duration_ms` | 34 | 1281 |
| `median_task_duration_ms` | 34 | 519 |
| `estimated_size_in_bytes_max` | 154009 (~150 KiB) | 9223372036854775808 (Long.MaxValue) |
| `estimated_row_count_max` | 2000 | null |
| `alerts` | many_exchanges, many_sorts | many_exchanges, many_sorts, **large_estimated_plan_size** |

Without statistics, `EXPLAIN COST` sizes `fact_sales` at the 8.0 EiB default
(`spark.sql.defaultSizeInBytes`). Join cardinalities then propagate to
~10^29 bytes, the `large_estimated_plan_size` alert fires at the Long.MaxValue
sentinel, and the run is ~14x slower. This is the scenario the interpreter
prompt targets ("statistics are likely missing — suggest `ANALYZE TABLE`"),
except it surfaces as the Long.MaxValue sentinel rather than `null`.

Primary suggestion for both variants: `ANALYZE TABLE ws2_poc.fact_sales
COMPUTE STATISTICS FOR ALL COLUMNS` (plus the three dimensions).

## Caveats found while interpreting (revisit against the assembler / WS2)

These are not query problems — they are data-quality issues in the assembled
package that would also degrade any automated interpretation:

1. **Inflated `indicator_summary` counts.** Both packages report
   `exchange_count = 18`, `broadcast_join_count = 18`, `sort_count = 10`.
   Neither executed plan supports that — each shows 3 BroadcastHashJoins,
   ~6 Exchanges, 1 Sort. The counts look summed across the
   formatted/executed/cost excerpts instead of counted once per query.

2. **`fact_sales` rowCount=0 with stats present.** In the with-stats
   `EXPLAIN COST`, `fact_sales` appears as
   `Statistics(sizeInBytes=1.0 B, rowCount=0)`, and `tables[].row_count` for
   `fact_sales` is `0` — yet the query produced a result. Either ANALYZE was
   not run on the fact table, or the captured stat is stale/empty.

3. **Zero shuffle bytes.** `shuffle_read_bytes` and `shuffle_write_bytes` are
   `0` in both variants despite shuffle Exchanges in the plan. Confirm
   whether Workstream 2 captures these metrics or whether they are genuinely
   zero for this small dataset.

## Confidence rationale

Both interpretations were assigned `confidence: "medium"`:

- **with stats** — plan, runtime metrics and stats are all present, but the
  inflated indicator counts and the `rowCount=0` fact statistic contradict
  the rest, so not `high`.
- **before stats** — plan and runtime metrics are fully captured, but
  statistics are absent, so not `high`; the diagnosis itself (missing stats)
  is unambiguous, so not `low`.
