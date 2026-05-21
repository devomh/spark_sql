# Lesson 9: Exercises

> **By the end of this lesson you will be able to:**
> *   Reason about plan snippets the way you would in a real performance investigation.
> *   Connect symptoms in a plan back to the right intervention from Lesson 7.
> *   Validate your understanding against worked answers.

Work through each exercise *before* opening the answer. The answers explain not just *what* the right call is, but *why* — that's where the learning is.

If you have PySpark installed (see [SETUP.md](SETUP.md)), you can reproduce each scenario locally.

---

## Exercise 1: Identify the Shuffle

Look at the following plan snippet:

```text
AdaptiveSparkPlan
+- HashAggregate(keys=[product_id#10], functions=[sum(amount#11)])
   +- Exchange hashpartitioning(product_id#10, 200)
      +- HashAggregate(keys=[product_id#10], functions=[partial_sum(amount#11)])
         +- Scan parquet default.sales
```

**Questions:**
1.  Where is the shuffle boundary?
2.  Why did Spark perform a shuffle here?
3.  What is the purpose of the `partial_sum` HashAggregate before the Exchange?

<details>
<summary><b>Answer</b></summary>

1.  The `Exchange hashpartitioning(product_id#10, 200)` node. Everything above it runs in a new stage from everything below it.
2.  The query is `GROUP BY product_id`. The same `product_id` may appear in many partitions across many executors. To compute a correct `sum` per product, all rows for the same `product_id` must end up on the same executor — that requires redistributing (shuffling) rows by `product_id`.
3.  This is the **partial / final aggregation** pattern. The `partial_sum` runs *before* the shuffle, on each input partition independently, summing locally. The shuffle then carries only the partial sums (one row per `product_id` per partition) instead of every raw row. The final `HashAggregate` adds the partials together. This dramatically reduces shuffle volume — often by orders of magnitude.

</details>

---

## Exercise 2: The Missing Broadcast

You are joining a large `fact_sales` table (1 billion rows) with a small `dim_product` table (100 rows). You expect a `BroadcastHashJoin`, but you see a `SortMergeJoin` in the plan.

**Questions:**
1.  What command can you run to help Spark understand the size of the `dim_product` table?
2.  If the table is a temporary view, what alternative can you use to guarantee broadcast?
3.  How would the plan look different if it successfully used a `BroadcastHashJoin`?

<details>
<summary><b>Answer</b></summary>

1.  `ANALYZE TABLE dim_product COMPUTE STATISTICS FOR ALL COLUMNS`. Without table-level `sizeInBytes` and `rowCount`, Spark falls back to a conservative "this is huge" assumption and avoids broadcast. (For Delta tables this is usually unnecessary — stats come for free on write.)
2.  Use an explicit hint: `sales.join(broadcast(dim_product), "product_id")` in DataFrames, or `/*+ BROADCAST(dim_product) */` in SQL. Hints bypass stats entirely — Spark will broadcast even if it has no size info.
3.  The plan would replace the `Exchange` + `SortMergeJoin` on the `dim_product` side with a `BroadcastExchange`, and the join itself becomes `BroadcastHashJoin`. The `fact_sales` side is no longer shuffled at all — its `Exchange` disappears. This is typically a 10×+ speedup on this kind of join.

</details>

---

## Exercise 3: Databricks Query Profile

You are looking at a Databricks Query Profile for a join. One side of the join has a "rows read" count of 1 million, but the output of the join is 100 million rows.

**Questions:**
1.  What is this phenomenon called?
2.  What does this usually indicate about your join keys?
3.  Is this a performance problem? Why?

<details>
<summary><b>Answer</b></summary>

1.  **Row explosion** (or sometimes "fan-out join"). The join is producing a cartesian-like multiplication.
2.  Your join key is **not unique** on at least one side — probably both. A 100× explosion typically means each key on one side matches ~100 rows on the other. Often this is unintentional: you forgot a `DISTINCT`, missed a join condition, or have duplicate keys in a dim table you assumed was unique.
3.  Yes — for two reasons. **(a)** Every downstream operator now processes 100× more data than the inputs implied; shuffles, sorts, and writes all balloon. **(b)** It often signals a *correctness* bug — the result has duplicate or fabricated rows. Always investigate row explosion before treating it as just a perf problem.

</details>

---

## Bonus Challenge: The "Ghost" Filter

You run a query with `WHERE sale_date = '2026-05-20'`. In the execution plan, you don't see a `Filter` operator anywhere, but the query finishes in seconds and returns the correct data.

**Question:** How is this possible?

<details>
<summary><b>Answer</b></summary>

The filter was **pushed all the way down into the scan** as a partition filter. Look at the `Scan` node — it will have a `PartitionFilters: [isnotnull(sale_date#5), (sale_date#5 = 2026-05-20)]` attribute. Because the table is partitioned by `sale_date`, Spark skips every folder except the matching one. No rows reach a `Filter` operator because no rows that *would* be filtered out are ever read. This is **partition pruning** — covered in Lessons 3 (Catalyst rule: predicate pushdown) and 8 (Databricks "Partitions Pruned" metric).

If the table used Delta with `dataSkippingNumIndexedCols` covering `sale_date`, the same skipping happens at the *file* level even without partitioning. Either way, the cheapest filter is one that runs as `PartitionFilters` or `PushedFilters` on the scan.

</details>

---

## Exercise 4: Picking a Hint

You have this query:

```python
result = (
    big_fact                      # 5 TB Delta table
    .join(medium_dim, "region_id")  # medium_dim is 300 MB
    .filter("year = 2026")
    .groupBy("region_name")
    .sum("revenue")
)
```

Spark uses a `SortMergeJoin` and the job takes 45 minutes, dominated by the shuffle of `big_fact` on `region_id`.

**Question:** Which intervention from [Lesson 7](07_intervention_toolkit.md) would you try first, and why?

<details>
<summary><b>Answer</b></summary>

`medium_dim` at 300 MB is too big for the default 10 MB auto-broadcast threshold, but it's small enough that broadcasting it to every executor is cheap *if* executor memory allows.

**First try:** raise the broadcast threshold for this session and re-run, e.g. `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "512m")`. If executors have ≥ a few GB of memory, this converts the join to `BroadcastHashJoin`, eliminating the 5 TB shuffle of `big_fact` — the dominant cost.

**Alternatively:** add an explicit `broadcast(medium_dim)` hint, which is more targeted (only this query) and survives even if someone later resets the threshold.

**Don't** start with `SHUFFLE_HASH` — `medium_dim` is small enough that broadcast is the clear win. Don't reach for skew tuning first either; nothing in the symptoms points to skew, just to an avoidable shuffle.

Verify with a re-`explain()` afterwards that the plan now shows `BroadcastHashJoin` and the `Exchange` on the `big_fact` side has disappeared.

</details>

---
**Navigation:** [Previous: Databricks Concepts](08_databricks_concepts.md) | [Back to Index](README.md)
