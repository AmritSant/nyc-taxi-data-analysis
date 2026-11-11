# NYC Taxi Data Analysis & Distributed Computing

A PySpark analysis project using NYC Taxi trip records. The project focuses on working with many Parquet files, normalizing inconsistent schemas, measuring distributed execution, and producing tipping summaries by trip distance and duration.

## What this project demonstrates

- Processed a dataset spanning **89 NYC Taxi files** with PySpark.
- Normalized conflicting numeric Parquet schemas before aggregation.
- Compared execution across **1-node, 2-worker, and 4-worker** configurations on Google Cloud Dataproc.
- Exported tip-by-distance and tip-by-duration summaries to Google Cloud Storage.

## Tech stack

Python, PySpark, Pandas, Google Cloud Dataproc, Google Cloud Storage, Parquet, and Bash.

## Repository layout

- `src/pyspark_aggregate.py` — schema-tolerant aggregation and summary output.
- `src/phase4_analysis.py` — tip-rate analysis by distance and duration buckets.
- `requirements.txt` — local Python dependency reference.

## Run locally or on Dataproc

The raw taxi files are not included because the public dataset is large. Download the data from the NYC TLC trip-records source, place it outside this repository, and provide its path or cloud URI.

```bash
pip install -r requirements.txt
spark-submit src/pyspark_aggregate.py \
  --input 'gs://YOUR_BUCKET/taxi/*.parquet' \
  --output 'gs://YOUR_BUCKET/results/aggregate'

spark-submit src/phase4_analysis.py \
  --input 'gs://YOUR_BUCKET/taxi/*.parquet' \
  --output 'gs://YOUR_BUCKET/results/phase4'
```

For a local run, replace the `gs://` paths with a path to local Parquet files. Cloud credentials and project-specific bucket names are intentionally not committed.
