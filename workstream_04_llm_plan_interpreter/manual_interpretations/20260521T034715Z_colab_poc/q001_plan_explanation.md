<!--
Worked example output of prompts/plan_explainer_prompt.md (plain-language
explanation), as opposed to the JSON tuning review from
prompts/plan_interpreter_prompt.md.

Produced by a subagent role-playing the model, given only the prompt template
and the two q001 llm_package.json files (with-stats and before-stats).
Regenerated against the self-contained revision of plan_explainer_prompt.md.
Draft - kept for later revision once the automated LLM caller exists.
-->

# q001 — Monthly Category Revenue

This query produces a monthly revenue summary: for each `year_month` / `category` / `region` combination it counts sale lines, counts distinct customers, sums units and net revenue, and computes an average sale amount. It joins the central sales fact table to three small reference tables and sorts the result by month, then by revenue.

### The tables involved

| Table | Role | Size | On-disk location | How the plan uses it |
|---|---|---|---|---|
| `ws2_poc.fact_sales` | fact (big central table) | Row count not reliably known — `tables[].row_count` is `0` and the cost estimate flips between two unreliable extremes (`sizeInBytes=1.0 B` with stats; `8.0 EiB` without). It is the one row-per-sale-line table, so it is the large side. | `/content/spark_sql_plan_poc/data/fact_sales`; scanned via `CatalogFileIndex` (catalog-managed table) | Read and streamed as the large side; the three dimensions are joined onto it. |
| `ws2_poc.dim_date` | dimension / lookup | 730 rows; `size_bytes` is `null`. Cost excerpt estimates ~22 KiB (with stats) / ~11 KiB (without). | `/content/spark_sql_plan_poc/data/dim_date`; scanned via `InMemoryFileIndex` (plain path-based read) | Broadcast — Spark copies this whole small table to every worker — and hash-joined on `date_id`. |
| `ws2_poc.dim_product` | dimension / lookup | 2,000 rows; `size_bytes` is `null`. Cost excerpt estimates ~68 KiB (with stats) / ~15 KiB (without). | `/content/spark_sql_plan_poc/data/dim_product`; scanned via `InMemoryFileIndex` | Broadcast to every worker and hash-joined on `product_id`. |
| `ws2_poc.dim_store` | dimension / lookup | 120 rows; `size_bytes` is `null`. Cost excerpt estimates ~3.9 KiB (with stats) / ~2.9 KiB (without). | `/content/spark_sql_plan_poc/data/dim_store`; scanned via `InMemoryFileIndex` | Broadcast to every worker and hash-joined on `store_id`. |

### Partitions

The fact table is partitioned. Its `description` names `year_month` as a Hive partition column (the folder layout on disk is split by `year_month`), and the scan reads it through a `CatalogFileIndex`, which is the catalog-managed index type used for partitioned tables.

However, **no partition pruning happened**. The `FileScan` for `fact_sales` shows `PartitionFilters: []`, meaning Spark did not skip any partition folders — the whole table is read off disk. The reason: the query's date window is expressed as `d.year_month BETWEEN '2024-04' AND '2025-03'`, but that filter applies to the **`dim_date` table's** `year_month` column, not the fact table's partition column. Even though both columns are named `year_month`, Spark cannot use a predicate on the date dimension to prune the fact table's partition folders. The `BETWEEN` filter only narrows rows *after* the join with `dim_date`; it does not reduce the files read from `fact_sales`. The only filters pushed into the `fact_sales` scan are non-partition `IsNotNull` / `returned_flag` predicates (`PushedFilters`), which is the row-level `f.returned_flag = false` condition plus join-key null checks.

The three dimension tables are scanned via `InMemoryFileIndex` (plain path-based reads) and are not partitioned.

### What it does, step by step

1. **Scan `fact_sales`** in full from disk (no partitions pruned), keeping `date_id`, `customer_id`, `product_id`, `store_id`, `quantity`, `net_amount`, `returned_flag`, `year_month`.
2. **Filter** the fact rows: drop returned sales (`returned_flag = false`) and rows with null join keys.
3. **Scan and filter the three dimensions**: `dim_date` (keeping only `year_month` between `2024-04` and `2025-03`), `dim_product`, and `dim_store`.
4. **Join `fact_sales` to `dim_date`** on `date_id` with a BroadcastHashJoin — the small date table is copied to every worker, so no shuffle of the large side is needed.
5. **Join the result to `dim_product`** on `product_id`, again as a broadcast hash join.
6. **Join the result to `dim_store`** on `store_id`, again as a broadcast hash join.
7. **Aggregate** by `year_month`, `category`, `region`: count sale lines, count distinct customers, sum quantity and net revenue, average net amount. Because of the `COUNT(DISTINCT customer_id)`, this runs as a multi-stage HashAggregate with a shuffle (`Exchange hashpartitioning`) that first groups by the keys plus `customer_id`.
8. **Sort** the final rows by `year_month` ascending and `net_revenue` descending, after a range-partitioning exchange to order data across partitions.

### Characteristics worth knowing

- **Cost statistics are not trustworthy.** With table statistics present, the optimizer estimates `fact_sales` and every join result at `sizeInBytes=1.0 B` / `rowCount=0` — clearly wrong, since the query returns aggregated revenue rows. Without statistics, the same relations are estimated at `8.0 EiB`, which trips a `large_estimated_plan_size` alert (`estimated_size_in_bytes_max = 9223372036854775808`). The fact table's `row_count` is also reported as `0` in `tables[]`. Treat the fact table's size and the join-cardinality estimates as unknown rather than as fact.
- **Plan shape is stable despite bad estimates.** In both captures all three joins are broadcast hash joins and no SortMergeJoin appears (`sort_merge_join_count = 0`); only the build side flips (`BuildLeft` with stats, `BuildRight` without). The dimensions are small enough to broadcast either way.
- **Many exchanges and sorts.** The package raises `many_exchanges` (18 vs threshold 2) and `many_sorts` (10 vs threshold 4) warnings. These counts reflect the broadcast exchanges feeding each join plus the multi-stage distinct-count aggregation and final sort.
- **The fact table has known data skew.** Its description states `product_id` is intentionally skewed — about 38% of rows reference `product_id=1` — and `promotion_id=0` ("no promotion") covers about 62% of rows. The `product_id` skew is relevant here because the query joins on `product_id`; the join itself is a broadcast (no shuffle of the fact side), but skew is a real property of this data worth knowing.
- **No partition pruning is the key takeaway.** The full `fact_sales` table is read because the date filter lives on `dim_date`, not on the fact table's `year_month` partition column (see Partitions above).
- **Plan excerpts are truncated.** `trimming_notes` reports each excerpt was truncated to 4,000 characters and file URIs/paths were masked. The operator tree above the joins (aggregates, exchanges, sort) and the four scans are fully visible; only deeper detail past the truncation point is cut, which does not change the explanation.
