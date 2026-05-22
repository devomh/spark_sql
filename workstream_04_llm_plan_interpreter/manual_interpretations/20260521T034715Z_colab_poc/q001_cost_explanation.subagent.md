<!--
Worked example output of prompts/cost_explainer_prompt.md (plain-language cost
explanation), produced by a subagent verification run via Metaprompt C.
Input: q001_monthly_category_revenue/explain_cost.txt (the with-stats capture).

Known issue in THIS run: the "Numbers you should not trust" section calls the
fact_sales rowCount=0 statistic "missing" and a "placeholder". That conflates a
present-but-wrong statistic with the exabyte-scale defaultSizeInBytes fallback —
two different failure modes. cost_explainer_prompt.md was hardened with an
explicit rule against this conflation immediately after this run. This file is
kept verbatim as the verification artifact that surfaced the slip.
-->

### What the optimizer expects this query to cost

This query builds a monthly category revenue report: it filters out returned sales, joins the sales records to three lookup tables — dates, products, and stores — then groups the result by month, category, and region to count sale lines, count distinct customers, and add up units and revenue, finally sorting the output. The honest answer to "what does the optimizer expect this to cost" is: it has no idea. Almost every step is stamped `sizeInBytes=1.0 B` with `rowCount=0` — the optimizer is effectively guessing the query touches nothing. The largest *trustworthy* estimated size belongs to the product lookup table at `150.4 KiB`, but that is a tiny side table, not the real workload. The main sales data, which dominates the actual cost, carries no usable estimate at all.

### Where the numbers come from

These are the base tables — the `Relation` lines at the bottom of the plan. Every estimate above them is built up from these, so their quality decides everything (`rowCount` means how many rows Spark expects; `sizeInBytes` is how big it thinks the data is).

- **`fact_sales`** — `Statistics(sizeInBytes=1.0 B, rowCount=0)` — **statistic looks wrong**. This is the central sales table the whole query is about; a real fact table is not 1 byte with 0 rows. The statistic is stale or was never collected.
- **`dim_date`** — `Statistics(sizeInBytes=62.7 KiB, rowCount=730)` — **looks right**. 730 rows is about two years of daily dates, and the size is plausible for a small date dimension.
- **`dim_product`** — `Statistics(sizeInBytes=150.4 KiB, rowCount=2.00E+3)` — **looks right**. `2.00E+3` means 2,000 products; a believable size for a product catalog.
- **`dim_store`** — `Statistics(sizeInBytes=6.4 KiB, rowCount=120)` — **looks right**. 120 stores at 6.4 KiB is a reasonable small store dimension.

### Numbers you should not trust

- **The `fact_sales` estimate is broken.** `rowCount=0` on the table the query is fundamentally about cannot be true — the table is not empty. Spark simply has no real statistics for it (no `ANALYZE TABLE` was ever run), so it fell back to a placeholder.
- **Every step that sits above `fact_sales` inherits the error.** Because the plan is read bottom-up, once the biggest input is guessed as 0 rows, the optimizer concludes everything downstream is also empty. That is why the filter, all three joins, the aggregate, and the final sort are all marked `rowCount=0` and `sizeInBytes=1.0 B`. Those are not real estimates — they are the broken `fact_sales` guess carried upward.
- In short, the only believable numbers in this whole plan are the three small dimension tables. The entire main pipeline — the part that actually costs something — is unestimated.

### What this means

Treat this cost plan as mostly unusable. The three lookup tables are measured fine, but they are not where the work is; the sales data that drives the query has no statistics, so the optimizer's picture of the query's size is wrong from the bottom up — and remember these are pre-run guesses, not timings, so nothing here tells you how long the query took anyway. The single most useful next step is to run `ANALYZE TABLE spark_catalog.ws2_poc.fact_sales COMPUTE STATISTICS` (ideally with column statistics for the join keys). Once Spark actually measures that table, the row counts and sizes for every join and aggregate above it will become meaningful, and a re-run of `EXPLAIN COST` will give estimates worth reading.
