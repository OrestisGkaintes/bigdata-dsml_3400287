# Large-Scale Data Management — Query Code

Spark implementations of Queries 1–4. Each query lives in its own folder
(`Query_1/`, `Query_2/`, …). Shared helpers (Spark session, dataset paths,
crime loader, timing) are in `common.py`, shipped to executors with `--py-files`.

## Running a query

All scripts take the crime-data format as an argument (`csv` default, or `parquet`):

```bash
spark-submit \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  --conf spark.executor.instances=<N> \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --py-files src/common.py src/Query_<X>/<script>.py csv
```

Executor counts per assignment requirement: Query 1 → 2, Query 2 → 4, Query 3 → 3.

Example (Query 2, SQL API):

```bash
spark-submit \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --py-files src/common.py src/Query_2/query2_sql.py csv
```

## Parquet conversion (Requirement 1)

Convert the crime CSVs to Parquet once, then run any query with `parquet`:

```bash
spark-submit --py-files src/common.py src/Query_1/convert_crime_to_parquet.py
```

## Reading results and timing

Jobs run in cluster mode, so output goes to the driver pod's log:

```bash
kubectl logs $(kubectl get pods --sort-by=.metadata.creationTimestamp \
  | grep <query-name> | tail -1 | awk '{print $1}') | cat
```

The execution time is printed as a `[TIMING] ...` line at the end of each run.