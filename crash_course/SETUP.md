# Setup: Run Spark Locally

This page gets you to your first `df.explain()` in under five minutes. The lessons that follow assume you have this working.

## Requirements

*   **Python 3.9+**
*   **Java 17** (Spark 3.5+ runs on Java 8/11/17; Java 17 is the simplest choice on modern systems)
*   ~500 MB free disk for the PySpark wheel

Check both:

```bash
python3 --version
java -version
```

## Install PySpark

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install "pyspark>=3.5,<4.0"
```

This pulls Spark itself bundled with the Python bindings — no separate Spark install needed.

## Your First Plan

Save this as `hello_plan.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("crash-course")
    .master("local[*]")  # use all local cores
    .getOrCreate()
)

# Synthetic data — no external files needed
df = spark.range(0, 1_000_000).selectExpr("id", "id % 100 AS bucket")

# Build a query
result = (
    df.filter("id > 500")
      .groupBy("bucket")
      .count()
)

# Print the physical plan WITHOUT executing
result.explain(mode="formatted")

# Now actually run it
result.show(5)

spark.stop()
```

Run it:

```bash
python3 hello_plan.py
```

You should see a `formatted` plan with operator IDs `(1)`, `(2)`, ... and then five rows of output. If that works, you are ready for Lesson 1.

## Useful `explain` Modes

| Mode | When to use it |
| :--- | :--- |
| `simple` | A one-line summary of the physical plan. Default. |
| `extended` | All four plans (parsed, analyzed, optimized, physical). Best for understanding Catalyst. |
| `formatted` | The physical plan in a numbered, readable form. Best for day-to-day work. |
| `cost` | Includes statistics (`sizeInBytes`, `rowCount`) on each node. Essential for Lesson 5. |

```python
result.explain(mode="extended")
result.explain(mode="cost")
```

## Troubleshooting

*   **`JAVA_HOME is not set`**: install Java 17 and `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` (macOS) or set it to your JDK path (Linux/Windows).
*   **`Python worker failed to connect`**: usually a Java/Python version mismatch. Confirm both versions above.
*   **Hangs on first run**: Spark is unpacking its bundled JARs. Subsequent runs are fast.

---
**Navigation:** [Back to Index](README.md) | [Next: Why Spark Planning is Hard](01_why_planning_is_hard.md)
