# Final assignment practical guide

This guide focuses on practical execution and performance-evaluation topics that directly affect the semester project deliverables.

It assumes you already completed:

- [04_remote-spark-kubernetes](../04_remote-spark-kubernetes/README.en.md)
- [05_cluster-queries-rdd-df-sql](../05_cluster-queries-rdd-df-sql/README.en.md)

## What this guide covers

1. How to read GeoJSON datasets into a Spark DataFrame.
2. How to change `spark-submit` settings for executors, cores, and memory.
3. How to measure execution time correctly for fair comparisons.
4. How to inspect and influence join strategies (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`).
5. How to keep experiments reproducible for reporting.

## Keep runs reproducible

Before each benchmarking session:

- use the same dataset and the same code revision
- change one factor at a time
- keep cluster load as stable as possible
- run each case multiple times (at least 3)
- report median runtime, not a single run

Use a clean shell:

```bash
deactivate 2>/dev/null || true
source ~/bigdata-env.sh
hash -r
command -v spark-submit
```

## 1. Reading the GeoJSON dataset into a DataFrame

The GeoJSON files provided in the datasets use the standard `FeatureCollection` structure, so the actual records are inside the `features` array. To work with them as a regular Spark DataFrame, read the file with `multiLine=true`, `explode` the `features` array, and then flatten the fields under `properties`.

```python
from pyspark.sql import functions as F

blocks_df = (
    spark.read
    .option("multiLine", "true")
    .json(geojson_path)
).selectExpr("explode(features) as features") \
 .select("features.*")

flattened_blocks_df = blocks_df.select(
    [
        F.col(f"properties.{col_name}").alias(col_name)
        for col_name in blocks_df.schema["properties"].dataType.fieldNames()
    ]
    + ["geometry"]
).drop("properties").drop("type")
```

With this pattern, the values under `properties` become top-level columns, while `geometry` remains available for the GeoJSON spatial data.

## 2. Changing executor settings

The assignment requires specific resource configurations.  
The safest practice is to apply `--conf` per run (instead of permanently editing default files).

### Per-run override (recommended)

```bash
spark-submit \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code/<your_script>.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER
```

## 3. Timing execution correctly

Do not treat shell timing around `spark-submit` as reliable Spark runtime.  
In cluster mode, `spark-submit` can finish submission while the application keeps running in Kubernetes.

### Recommended method A (primary): measure inside Python code

For assignment comparisons, prefer `perf_counter()` around the final Spark action.

```python
from time import perf_counter

start = perf_counter()
result_df.write.mode("overwrite").csv(output_path)  # or another final action
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
```

If you use this method, keep exactly the same timing boundary across all implementations (RDD / DF / SQL).

### Recommended method B (validation): measure application duration from Spark

Use Spark runtime evidence (Spark UI / History Server), not terminal wall-clock.

1. Submit with a unique app name.
2. Wait for completion in the cluster.
3. Record `Application ID` and `Duration` from the History Server, when available.

Example submit:

```bash
spark-submit \
  --conf spark.app.name=Q2_DF_run1_4x1c_2g \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code/Q2_df.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER
```

Then use the `Duration` value from History Server in your comparison table, if the service is available.

## 4. Join strategies: inspect and experiment

### Inspect what Catalyst chose

For DataFrame:

```python
result_df.explain("formatted")
```

For SQL:

```python
spark.sql("<your query>").explain("formatted")
```

In the physical plan, look for nodes such as:

- `BroadcastHashJoin`
- `SortMergeJoin`
- `ShuffledHashJoin`
- `BroadcastNestedLoopJoin` (often in non-equi or cross joins)

### Force strategy with hints

DataFrame API:

```python
from pyspark.sql.functions import broadcast

joined = left.join(broadcast(right), "zip_code")
# or:
joined = left.hint("merge").join(right, "zip_code")
joined = left.hint("shuffle_hash").join(right, "zip_code")
joined = left.hint("shuffle_replicate_nl").join(right, "zip_code")
```

Spark SQL:

```sql
SELECT /*+ BROADCAST(dim) */ ...
SELECT /*+ MERGE(a, b) */ ...
SELECT /*+ SHUFFLE_HASH(a) */ ...
SELECT /*+ SHUFFLE_REPLICATE_NL(dim) */ ...
```

## 5. Final checklist before submission

- Confirm all API variants requested by the assignment are implemented.
- Confirm there is execution evidence for each required resource configuration.
- Include runtime comparison tables and concise technical commentary.
- Keep a stable repository commit for the oral examination.
