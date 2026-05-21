# Lesson 1: Why Spark Planning is Hard

> **By the end of this lesson you will be able to:**
> *   Explain why a distributed engine over external storage can't plan like an OLTP database.
> *   Name the five foundational challenges that shape every Spark optimization decision.
> *   Predict the *kinds* of decisions Spark will get wrong, so you know what to watch for in plans.

If you are coming from PostgreSQL, MySQL, or Oracle, Spark will feel strange. A query that a traditional database plans in microseconds may take Spark several seconds — and produce a worse plan. This is not laziness on Spark's part; it's a consequence of the environment it runs in.

This lesson is the **"why"** behind the entire rest of the course. Read it first, then the mechanics in Lessons 2–5 will make sense.

---

## The Fundamental Difference

| | Traditional OLTP DB | Spark SQL |
| :--- | :--- | :--- |
| **Owns its data?** | Yes — every write goes through the engine. | No — queries arbitrary files on cloud storage. |
| **Stats kept?** | Updated incrementally on every `INSERT`. | Computed only when you ask (`ANALYZE TABLE`) or, for Delta, on write. |
| **Knows row locations?** | Yes — via B-Trees and indexes. | Only roughly, via partitioning and file-level min/max. |
| **Cluster topology?** | One node, fixed. | Dozens of executors, work redistributed by shuffle. |
| **Plan once execution starts?** | Fixed. | Can be re-optimized at runtime (AQE — Lesson 6). |

Every challenge below is a direct consequence of the right-hand column.

---

## Challenge 1: The "Black Box" of External Storage

When Spark sees `spark.read.parquet("s3://bucket/data/")`, it has to discover what's there.

*   **What it knows for free:** Parquet/Delta file footers — so `sizeInBytes` is cheap.
*   **What it does *not* know for free:** how many rows, column distributions, null counts. Without `ANALYZE TABLE` (or Delta's auto-collected stats), the cost-based optimizer is flying half-blind.
*   **Consequence:** Spark may pick the wrong join strategy because its estimate of "how big will this table be?" is off by orders of magnitude.

## Challenge 2: The Compounding Error of Join Estimates

Choosing the right join strategy is the single most consequential decision the optimizer makes.

*   **The dilemma:** Guess that a table is small (≤ 10 MB) → broadcast it. If it's actually 2 GB, every executor crashes with an `OutOfMemoryError`. Guess that a small table is large → an unnecessary, expensive shuffle.
*   **Why it compounds:** Estimating the size of a base table is hard. Estimating the size *after* three filters and two joins is mathematically much harder. Each step multiplies the uncertainty. By the fourth operator, the estimate may be off by 1000×.

This is sometimes called the *cardinality death spiral* — and it's why AQE (Lesson 6) is so important.

## Challenge 3: Data Skew — The Silent Killer

Spark divides data into partitions and processes them in parallel. This only works if the partitions are roughly the same size.

*   **The problem:** If 90% of your sales rows are for `store_id = 1`, the shuffle for a `GROUP BY store_id` will send 90% of the data to a single partition.
*   **The result:** One executor grinds for hours; the other 99 finish in seconds and sit idle. CPU is wasted, the job's wall time is dominated by the one straggler, and if the partition exceeds executor heap, it **spills to disk** — orders of magnitude slower than RAM.
*   **Why static optimizers can't see it:** they assume uniform distribution. Only runtime metrics expose skew.

## Challenge 4: The Mystery of UDFs

If you write `WHERE age > 18`, Catalyst can estimate how many rows pass.

If you write `WHERE my_python_function(email) = True`, Catalyst sees opaque bytecode it cannot reason about.

*   **It can't estimate selectivity** — is this 1% or 99% of rows? That decides whether to apply the filter before or after a join.
*   **It can't push the filter into Parquet** — the storage layer doesn't know your Python.
*   **There's a serialization tax** — for Python UDFs, rows cross the JVM↔Python boundary, get pickled, get processed, get pickled back. This is often 10–100× slower than an equivalent SQL expression.

## Challenge 5: The Partition Sizing Problem

Spark defaults to `spark.sql.shuffle.partitions = 200`. This is *one number for every query in your application*, regardless of data size.

*   **Too many partitions for small data:** filter a dataset down to 1,000 rows → 200 partitions of 5 rows each. The overhead of launching 200 tasks dominates the actual work.
*   **Too few partitions for big data:** 10 TB across 200 partitions = 50 GB per task. Executors OOM.
*   **The right number** depends on the data *after* every filter and join — which static planning can't know.

---

## What This Means for You

Each of these challenges shapes what you'll learn next:

*   **Lessons 2–4** teach you the mechanics — what Spark actually does and how to read it in plans — so you can *see* the symptoms of the challenges above.
*   **Lesson 5** covers statistics, which directly attacks Challenge 1.
*   **Lesson 6** covers AQE, Spark's runtime defence against Challenges 2, 3, and 5.
*   **Lesson 7** is the **Intervention Toolkit** — the specific hints, configs, and patterns to use when the optimizer still gets it wrong.

> [!TIP]
> **Mental model:** Spark planning is "best-effort with a safety net." The first plan is a guess; AQE corrects it where it can; your hints and configs are the final lever.

---
**Navigation:** [Previous: Index](README.md) | [Next: Spark Execution Model](02_execution_model.md)
