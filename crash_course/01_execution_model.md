# Lesson 1: Spark Execution Model

To understand Spark SQL plans, you must first understand the infrastructure and execution hierarchy that runs them. Spark is a distributed engine, meaning it splits work across many machines.

## The Infrastructure Hierarchy

### 1. The Driver (The Brain)
The **Driver** is the process where your `SparkSession` lives. It is responsible for:
*   Maintaining the state of the Spark application.
*   Interacting with the Cluster Manager.
*   Analyzing, distributing, and scheduling work across the executors.
*   Collecting results (when you call `.collect()`).

> [!WARNING]
> **Driver Bottleneck:** If you call `.collect()` on a massive dataset, all that data travels to the Driver. If the Driver doesn't have enough memory, it will crash with an `OutOfMemoryError` (OOM).

### 2. The Executors (The Workers)
**Executors** are JVM processes that run on worker nodes. They are responsible for:
*   Executing the tasks assigned by the Driver.
*   Storing data in-memory or on-disk (caching).
*   Reporting the state of computation back to the Driver.

### 3. The Cluster Manager
The "orchestrator" that allocates resources. In this course, we will emulate a **Standalone** cluster manager on a single machine, but in production, this could be **YARN**, **Kubernetes**, or the **Databricks** control plane.

---

## The Execution Hierarchy

When you call an **Action**, Spark triggers a **Job**. That Job is broken down into a hierarchy:

### 1. Job
A Job is the top-level unit of work. One Action = One Job.
*   *Example:* `df.write.save()` or `df.count()`.

### 2. Stage
A Job is divided into **Stages**. A new Stage is created whenever data needs to be **shuffled** across the network.
*   **Narrow Transformations** (no shuffle): `filter`, `select`, `map`. These stay in the same stage.
*   **Wide Transformations** (shuffle): `groupBy`, `join`, `distinct`. These force a new stage.

### 3. Task
A Stage is divided into **Tasks**. A Task is the smallest unit of work. 
*   One Task runs on one Core and processes one **Partition** of data.
*   If you have 100 partitions, Spark will launch 100 tasks for that stage.

---

## Lazy Evaluation & Lineage

Spark doesn't execute code line-by-line. Instead, it builds a **Logical Plan** (a "lineage") of transformations. 

```python
# Nothing happens yet
df = spark.read.parquet("sales.parquet")
df_filtered = df.filter("amount > 100")
df_grouped = df_filtered.groupBy("category").sum("amount")

# Execution starts NOW
df_grouped.show() 
```

### Why Lazy?
Laziness allows the **Catalyst Optimizer** to look at the entire chain of events and optimize it (e.g., "I see you only need 2 columns from this 100-column table; I'll only read those 2").

---

## The Shuffle: The Performance Killer

A **Shuffle** (appearing as `Exchange` in plans) is the process of redistributing data so that related data (e.g., all sales for "Product A") ends up on the same executor.

**Why is it slow?**
1.  **Disk I/O:** Data is written to local disk on the source executor.
2.  **Network I/O:** Data is sent over the network to the destination executor.
3.  **Serialization:** Converting objects to bytes and back.

> [!TIP]
> **Optimization Goal:** Many Spark tuning efforts boil down to: **Reduce the number of shuffles and the amount of data being shuffled.**

---
**Navigation:** [Previous: Index](README.md) | [Next: Spark SQL Planning Model](02_planning_model.md)
