# Lesson 6: Exercises

Test your understanding of Spark SQL plans with these guided analysis questions.

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

**Hints:**
*   Look for the `Exchange` node.
*   Think about what happens when data is grouped by a key that is spread across multiple partitions.
*   Recall the "Partial/Final" aggregation pattern from Lesson 3.

---

## Exercise 2: The Missing Broadcast

You are joining a large `fact_sales` table (1 billion rows) with a small `dim_product` table (100 rows). You expect a `BroadcastHashJoin`, but you see a `SortMergeJoin` in the plan.

**Questions:**
1.  What command can you run to help Spark understand the size of the `dim_product` table?
2.  If the table is a temporary view, what PySpark configuration might you check to ensure it can be broadcast?
3.  How would the plan look different if it successfully used a `BroadcastHashJoin`?

**Hints:**
*   Check Lesson 4 for the "Golden Command."
*   Recall the `spark.sql.autoBroadcastJoinThreshold` setting.
*   Remember that `Exchange` nodes are the hallmark of non-broadcast joins.

---

## Exercise 3: Databricks Query Profile

You are looking at a Databricks Query Profile for a join. One side of the join has a "rows read" count of 1 million, but the output of the join is 100 million rows.

**Questions:**
1.  What is this phenomenon called (where the join produces more rows than its input)?
2.  What does this usually indicate about your join keys?
3.  Is this a performance problem? Why?

**Hints:**
*   Think about "many-to-many" relationships.
*   What happens if a join key is not unique in either table?
*   Consider the amount of data being shuffled vs. the amount being written to the next stage.

---

## Bonus Challenge: The "Ghost" Filter

You run a query with `WHERE sale_date = '2026-05-20'`. In the execution plan, you don't see a `Filter` operator anywhere, but the query finishes in seconds and returns the correct data.

**Question:**
*   How is this possible? Look for terms in Lesson 2 and Lesson 5.

**Hint:**
*   Check the `Scan` node for `PartitionFilters`.

---

## Answers and Discussion

*Coming soon in the next lab session! We will verify these answers using real plans in our Colab notebook.*

---
**Navigation:** [Previous: Databricks Concepts](05_databricks_concepts.md) | [Next: Why Spark Planning is Hard](07_planning_challenges.md)
