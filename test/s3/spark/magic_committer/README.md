# Spark S3A magic committer on SeaweedFS

A docker compose playground for writing to SeaweedFS from Spark with the S3A
magic committer (`spark.hadoop.fs.s3a.committer.magic.enabled=true`), on
Spark 4.1.3 (Hadoop 3.4.2, AWS SDK v2).

With the magic committer, each task writes its files straight to their final
S3 keys as multipart uploads but does not complete them. The upload details go
into `.pending` files under `<table>/__magic_job-<id>/`. At job commit the
driver completes every upload, writes a JSON `_SUCCESS` manifest, and deletes
the `__magic_job-<id>` directory. Nothing is renamed, and nothing is visible
until the job commits.

`TestSparkS3AMagicCommitter` in `../magic_committer_test.go` runs the baseline
checks in CI, on Spark 3.5.1 like the other Spark tests there.

## Layout

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | `seaweedfs` (`weed mini` built from this checkout) and `spark` (Spark 4.1.3) |
| `Dockerfile.spark` | `apache/spark:4.1.3` plus `hadoop-aws`, the AWS SDK v2 bundle and `spark-hadoop-cloud`, all sha256-pinned |
| `spark-defaults.conf` | S3A endpoint/credentials and the magic committer settings, picked up by every Spark tool in the container |
| `s3.json` | SeaweedFS S3 identity `spark` / `spark-secret` |
| `magic_committer_job.py` | Writes a partitioned table (overwrite, then append), optionally failing tasks on purpose, and checks the commit |

## Prerequisites

- Docker with the compose plugin
- Go, to build `weed` from this checkout

## Quick start

```bash
cd test/s3/spark/magic_committer
make up     # builds ./weed, builds the Spark image, starts both, creates bucket "spark"
make test   # runs magic_committer_job.py with spark-submit
make test-task-retry      # first attempt of two tasks raises mid-write, Spark retries them
make test-executor-retry  # a task SIGKILLs its executor mid-write, Spark retries on a new one
make down   # stop; `make clean` also drops the SeaweedFS data volume
```

The first `make up` downloads about 650 MB of jars from Maven Central, most of
it the AWS SDK v2 bundle. If you
are rate limited or behind a mirror, set `MAVEN_REPO`:

```bash
MAVEN_REPO=https://maven-central.storage-download.googleapis.com/maven2 make up
```

After changing SeaweedFS code, `make weed && docker compose restart seaweedfs`
picks up the new binary.

## What `make test` checks

For both the overwrite and the append write:

- `_SUCCESS` is a JSON manifest with `"committer": "magic"`. The classic
  `FileOutputCommitter` writes an empty `_SUCCESS`, so this proves the magic
  committer ran.
- Every file listed in the manifest exists and is not empty. The script also
  prints the largest multipart part count it saw (from the `-N` ETag suffix).
- Only files listed in the manifest (plus what the table held before) are
  visible, so nothing a failed attempt wrote leaks into the table.
- No files remain under `__magic_job-<id>`.
- No multipart uploads remain open under the table prefix.

After both writes it reads the table back and checks the row count.

Sample output:

```text
== overwrite ==
  PASS  _SUCCESS written by the magic committer (committer='magic')
  PASS  _SUCCESS lists 16 committed files
  PASS  every committed file exists
  PASS  no committed file is empty
        committed 210013876 bytes in 16 files, up to 3 multipart parts per file
        committer_commits_completed = 16
        ...
  PASS  only committed files are visible
  PASS  no files left under __magic
  PASS  no pending multipart uploads under magic-committer/output/
...
MAGIC COMMITTER TEST PASSED
```

## Task failure and retry

`--inject` makes the first attempt of some tasks die after 60% of their rows,
while each of their open files has multipart parts uploaded. The writes then
stream rows into one open file per partition (`plannedWrite` off, concurrent
writers on) and flush 1 MiB Parquet row groups, so the failure really lands
mid-upload. On top of the checks above, each write checks that the injected
attempts failed and that their uploads were aborted.

| Target | Failure | Who cleans up | Master |
| --- | --- | --- | --- |
| `make test-task-retry` | tasks 0 and 2 raise on attempt 0 | Spark aborts the task; the committer aborts the uploads in its `.pending` files | `local[4,4]` (4 attempts per task) |
| `make test-executor-retry` | task 0 SIGKILLs its executor JVM on attempt 0, also losing the task next to it | nobody on the executor; job cleanup lists the table's open uploads and aborts them | `local-cluster[2,2,1024]` (two executor processes) |

```text
== overwrite ==
  PASS  1 task attempt(s) failed by executor-kill and were retried: stage0-task0
  PASS  8 multipart upload(s) left by failed attempts were aborted
  PASS  _SUCCESS written by the magic committer (committer='magic')
  ...
  PASS  only committed files are visible
  PASS  no pending multipart uploads under executor-kill/output/
```

The failed attempts drop markers in `/tmp` inside the spark container, so
`--inject` only works with the masters above, where driver and executors share
that container.

The job takes `--target`, `--rows` (default 3,000,000) and `--tasks`
(default 4):

```bash
make test ARGS="--rows 10000000 --tasks 8 --target s3a://spark/big/output"
```

`fs.s3a.multipart.size` is set to 5M, the smallest part S3 allows, so the
default run already uploads several parts per file.

## Interactive use

```bash
make pyspark       # or: make spark-shell / make shell
```

```python
spark.range(100).write.mode("overwrite").parquet("s3a://spark/demo")
```

Spark skips files starting with `_` when it reads, so look at the manifest
through the filer instead:

```bash
curl -s http://localhost:8888/buckets/spark/demo/_SUCCESS
```

To compare committers, override them per run:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  /opt/jobs/magic_committer_job.py --target s3a://spark/directory-committer/output
```

The job then reports a failed `committer` check, which is expected:
`directory` and `partitioned` (the staging committers) or `file` (the classic
`FileOutputCommitter`) are not `magic`.

## Using your own Spark

Point Spark 4.1.x (Hadoop 3.4.2) at `http://localhost:8333` with the settings
from `spark-defaults.conf`. It needs these jars on the classpath:

- `org.apache.hadoop:hadoop-aws:3.4.2`
- `software.amazon.awssdk:bundle:2.29.52`
- `software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.0`
- `org.apache.spark:spark-hadoop-cloud_2.13:4.1.3`

These are the versions `spark-hadoop-cloud` 4.1.3 resolves to, and the ones
tested here.

The two `spark.sql.*` settings are what make Spark SQL use the S3A committer.
Without them Parquet writes go through `ParquetOutputCommitter`, a
`FileOutputCommitter` subclass, whatever `fs.s3a.committer.name` says.

Ports can be moved with `S3_PORT`, `FILER_PORT`, `MASTER_PORT` and
`SPARK_UI_PORT`. The filer UI at http://localhost:8888/buckets/spark/ shows
the bucket contents.

## Notes

- After job cleanup an empty `__magic_job-<id>/` directory can still be listed
  for a couple of minutes: SeaweedFS removes empty folders asynchronously. The
  job prints a note when it sees one, and only fails if files remain inside.
- Keep `spark-hadoop-cloud` in step with Spark, and `hadoop-aws` in step with
  the Hadoop client bundled in Spark (3.4.2 for Spark 4.1.3).
