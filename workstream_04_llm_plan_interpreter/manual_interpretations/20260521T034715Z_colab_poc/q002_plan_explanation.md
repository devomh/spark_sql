<!--
Worked example output of prompts/plan_explainer_prompt.md (plain-language
explanation), as opposed to the JSON tuning review from
prompts/plan_interpreter_prompt.md.

Produced from q002_customer_window_revenue/llm_package.json (this query has a
single capture - no before-stats variant).
Draft - kept for later revision once the automated LLM caller exists.
-->

# q002 — Customer Window Revenue

This query finds each customer's three highest-revenue days. It first rolls
sales up to one row per customer per day, then uses window functions to
compute a 7-day rolling revenue total and to rank each customer's days by
revenue, keeping the top 3. It joins the central sales fact table to two
small reference tables and returns the first 500 rows ordered by customer.

### The tables involved

| Table | Role | Size | On-disk location | How the plan uses it |
|---|---|---|---|---|
| `ws2_poc.fact_sales` | fact (big central sales table) | Not reliably known — `tables[].row_count` is `0` and the cost excerpt estimates `sizeInBytes=1.0 B` / `rowCount=0`, which cannot be right since the query returns rows. It is the one row-per-sale-line table, so it is the large side. | `/content/spark_sql_plan_poc/data/fact_sales`; scanned via `CatalogFileIndex` (a catalog-managed, partitioned table). | Read and streamed as the large side; the two dimensions are joined onto it. |
| `ws2_poc.dim_customer` | dimension (customer lookup) | 25,000 rows; `size_bytes` is `null`. Cost excerpt estimates ~2.3 MiB raw (~1.3 MiB after column pruning). | `/content/spark_sql_plan_poc/data/dim_customer`; scanned via `InMemoryFileIndex` (a plain path-based read, not catalog-partitioned). | Broadcast — Spark copies the whole small table to every worker — and hash-joined on `customer_id`. |
| `ws2_poc.dim_date` | dimension (date lookup) | 730 rows; `size_bytes` is `null`. Cost excerpt estimates ~62.7 KiB raw (~11.4 KiB after pruning). | `/content/spark_sql_plan_poc/data/dim_date`; scanned via `InMemoryFileIndex` (path-based read). | Broadcast to every worker and hash-joined on `date_id`. |

### Partitions

`fact_sales` **is** partitioned. Its table `description` names `year_month`
as a "Hive partition" — rows are stored in separate folders on disk, one per
`year_month` value — and the scan reads it through a `CatalogFileIndex`, the
catalog-managed index type used for partitioned tables.

**No partition pruning happened.** The `FileScan` for `fact_sales` shows
`PartitionFilters: []`, so the **entire `fact_sales` table is read off disk**,
every `year_month` folder included. The reason: the query's date filter is
`d.sale_date >= DATE '2024-07-01'`, which applies to the **`dim_date`
dimension's** `sale_date` column, not to the fact table's `year_month`
partition column. Spark cannot use a predicate on the date dimension to skip
fact-table partition folders, so the date cutoff only narrows rows *after*
the join to `dim_date`. The only filters pushed into the fact scan
(`PushedFilters`) are not-null checks and `returned_flag = false`.

The two dimension tables are scanned via `InMemoryFileIndex` (plain
path-based reads) and are not partitioned.

### What it does, step by step

1. **Scan `fact_sales`** in full from disk (no partitions pruned), keeping `date_id`, `customer_id`, `net_amount`, `returned_flag`, `year_month`.
2. **Filter** the fact rows: drop returned sales (`returned_flag = false`) and rows with null join keys.
3. **Scan and filter the dimensions**: `dim_customer` (non-null `customer_id`) and `dim_date` (keeping `sale_date >= 2024-07-01`).
4. **Join `fact_sales` to `dim_customer`** on `customer_id` with a BroadcastHashJoin — the small table is copied to every worker, so the large side is not shuffled.
5. **Join the result to `dim_date`** on `date_id`, again as a broadcast hash join.
6. **Aggregate to one row per customer per day** — `GROUP BY customer_id, segment, region, sale_date`, counting sale lines and summing net revenue. This runs as a two-stage HashAggregate with a shuffle (`Exchange hashpartitioning`) on those group keys.
7. **Shuffle by `customer_id`** (`Exchange hashpartitioning(customer_id, 8)`) and sort, so all of a customer's days sit together — this feeds both window functions.
8. **Window 1 — rolling revenue:** sum `daily_revenue` over the 6 preceding rows plus the current row, per customer ordered by `sale_date`.
9. **Sort and trim:** re-sort by `(customer_id, daily_revenue DESC, sale_date DESC)`, and a `WindowGroupLimit` keeps only the top 3 rows per customer *before* the ranking window runs.
10. **Window 2 — ranking:** assign `ROW_NUMBER()` per customer by descending daily revenue, then `Filter` to `revenue_day_rank <= 3`.
11. **Return the top 500 rows** ordered by `customer_id, revenue_day_rank`, via `TakeOrderedAndProject` (an efficient combined ORDER BY + LIMIT).

### Characteristics worth knowing

- **Two window functions, one shuffle.** Both windows partition by `customer_id`, so Spark shuffles the data by `customer_id` only once and reuses it. Each window still needs its own `Sort`, because they order rows differently (`sale_date` for the rolling sum, `daily_revenue DESC` for the rank).
- **`WindowGroupLimit` optimization.** Because the final result only keeps `revenue_day_rank <= 3`, Spark pushes that limit into the ranking step (`WindowGroupLimit ... row_number(), 3`), so it keeps just 3 rows per customer instead of ranking every day. This is why the otherwise window-heavy query stays cheap (1229 ms, single task).
- **Cost statistics are not trustworthy.** `fact_sales` reports `row_count: 0` in `tables[]` and `rowCount=0 / sizeInBytes=1.0 B` in the cost excerpt, even though the query produces ranked revenue rows. The only meaningful size estimate in the package is `dim_customer` (~2.3 MiB / 25,000 rows); treat the fact table's size as unknown.
- **Whole-table read of the fact table.** Because partition pruning is lost (see Partitions), every `year_month` folder of `fact_sales` is read regardless of the `2024-07-01` cutoff. The date filter trims rows only after the join, not the files scanned.
- **Plan alerts.** The package raises `many_exchanges` (12 vs. a threshold of 2) and `many_sorts` (16 vs. 4). These reflect the broadcast/shuffle exchanges and the per-window sorts — but the raw counts look inflated versus the executed plan, which shows 2 Window operators, ~4 Sorts and ~4 Exchanges. The counts appear to be summed across the formatted/executed/cost excerpts.
- **Efficient final step.** `TakeOrderedAndProject(limit=500, ...)` handles `ORDER BY customer_id, revenue_day_rank LIMIT 500` without a separate full sort and range-partitioning exchange.
- **Truncation note.** `trimming_notes` reports each excerpt was truncated to 4,000 characters and file URIs masked. The operator tree (scans, two broadcast joins, two-stage aggregate, two windows, final top-N) is fully visible; only deeper detail past the truncation point is cut, which does not change the explanation.
