# Lesson 3: Query Plan Reading

Reading a Spark execution plan is like reading a tree from the **bottom up**. The data flows from the leaves (Scans) toward the root (Result).

## Common Operators: A Deep Dive

### 1. Data Access (The Leaves)
*   **FileScan / BatchScan**: Reading from a file (Parquet, CSV, Delta).
    *   **PushedFilters**: Look here! This shows which filters Spark passed down to the storage layer to skip data.
    *   **PartitionFilters**: Shows if Spark is skipping entire folders (partitions) based on your query.
    *   **DataFilters**: Filters that Spark has to apply *after* reading the data.

### 2. Basic Transformations
*   **Project**: Selecting, renaming, or creating columns. High-count `Project` nodes are fine; they are very cheap.
*   **Filter**: Keeping rows that match a condition. If this is high up in the plan (far from the Scan), it means Spark had to read the data before filtering it.
*   **Sort**: Arranging rows. `Sort` is expensive and often causes a shuffle.

### 3. Aggregations (The Summarizers)
Aggregations usually happen in two phases: **Partial** and **Final**.
*   **HashAggregate**: Uses a hash table. Very fast if the keys fit in memory.
*   **SortAggregate**: Requires data to be sorted by the grouping key first. Used for very large grouping keys or when memory is tight.
*   **ObjectHashAggregate**: Used when grouping by complex objects (e.g., strings or custom types).

### 4. Shuffles (The Red Flags)
*   **Exchange**: This is a **Shuffle**. Data is being redistributed across partitions.
    *   `hashpartitioning(id, 200)`: Rows with the same `id` go to the same partition.
    *   `roundrobinpartitioning(200)`: Rows are spread out evenly (good for load balancing, bad for joins).
*   **BroadcastExchange**: A special shuffle where a small table is sent to **every** executor. This avoids a large-scale shuffle during a join.

---

## Join Strategies: The "Make or Break" of Spark

Spark has several ways to join tables. Choosing the right one is critical.

| Strategy | Performance | Why use it? |
| :--- | :--- | :--- |
| **Broadcast Hash Join (BHJ)** | 🚀 **Fastest** | One table is small enough to fit in the memory of every executor (default < 10MB). No shuffle! |
| **Sort Merge Join (SMJ)** | 🐢 **Slow** | Both tables are large. Requires shuffling both sides by the join key and then sorting them. The standard "big table" join. |
| **Shuffled Hash Join (SHJ)** | 🏃 **Medium** | Both tables are large, but Spark can join them by shuffling without a full sort. Often faster than SMJ but uses more memory. |
| **Broadcast Nested Loop Join** | 💀 **Deadly** | A "cartesian product" fallback. Used for non-equi joins (e.g., `WHERE a.id > b.id`). Avoid this at all costs on large data. |

---

## Whole-Stage Code Generation (WSCG)

You will often see `*` symbols in a text plan (e.g., `*Project`, `*Filter`).
This means Spark has compiled these operators into a single piece of highly optimized Java code. 
Instead of passing rows between operators one-by-one, Spark processes them in a single loop. This is a massive performance boost!

---

## Pro-Tip: How to Read the Output

When you run `df.explain(mode="formatted")`, pay attention to the **Operator IDs**.

```text
(1) Scan parquet default.sales [codegen id : 1]
(2) Filter [codegen id : 1]
(3) Project [codegen id : 1]
(4) Exchange
(5) HashAggregate [codegen id : 2]
```

Everything with `codegen id : 1` is running in a single, fast Java loop. The `Exchange` at step (4) breaks that loop, and a new one starts at step (5).

---
**Navigation:** [Previous: Spark SQL Planning Model](02_planning_model.md) | [Next: Cost and Statistics](04_cost_and_statistics.md)
