# Spark S3A magic committer on SeaweedFS

A docker compose playground for writing to SeaweedFS from Spark with the S3A
magic committer (`spark.hadoop.fs.s3a.committer.magic.enabled=true`).

With the magic committer, each task writes its files straight to their final
S3 keys as multipart uploads but does not complete them. The upload details go
into `.pending` files under `<table>/__magic/`. At job commit the driver
completes every upload, writes a JSON `_SUCCESS` manifest, and deletes
`__magic`. Nothing is renamed, and nothing is visible until the job commits.

The automated version of these checks is `TestSparkS3AMagicCommitter` in
`../magic_committer_test.go`. It runs in CI with the other Spark tests.

## Layout

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | `seaweedfs` (`weed mini` built from this checkout) and `spark` (Spark 3.5.1) |
| `Dockerfile.spark` | `apache/spark:3.5.1` plus `hadoop-aws`, the AWS SDK bundle and `spark-hadoop-cloud`, all sha256-pinned |
| `spark-defaults.conf` | S3A endpoint/credentials and the magic committer settings, picked up by every Spark tool in the container |
| `s3.json` | SeaweedFS S3 identity `spark` / `spark-secret` |
| `magic_committer_job.py` | Writes a partitioned table (overwrite, then append) and checks the commit |

## Prerequisites

- Docker with the compose plugin
- Go, to build `weed` from this checkout

## Quick start

```bash
cd test/s3/spark/magic_committer
make up     # builds ./weed, builds the Spark image, starts both, creates bucket "spark"
make test   # runs magic_committer_job.py with spark-submit
make down   # stop; `make clean` also drops the SeaweedFS data volume
```

The first `make up` downloads about 290 MB of jars from Maven Central. If you
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
- No files remain under `__magic`.
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
  PASS  no files left under __magic
  PASS  no pending multipart uploads under magic-committer/output/
...
MAGIC COMMITTER TEST PASSED
```

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

Point any Spark 3.5.x with Hadoop 3.3.4 at `http://localhost:8333` with the
settings from `spark-defaults.conf`. It needs these jars on the classpath:

- `org.apache.hadoop:hadoop-aws:3.3.4`
- `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- `org.apache.spark:spark-hadoop-cloud_2.12:3.5.1`

The two `spark.sql.*` settings are what make Spark SQL use the S3A committer.
Without them Parquet writes go through `ParquetOutputCommitter`, a
`FileOutputCommitter` subclass, whatever `fs.s3a.committer.name` says.

Ports can be moved with `S3_PORT`, `FILER_PORT`, `MASTER_PORT` and
`SPARK_UI_PORT`. The filer UI at http://localhost:8888/buckets/spark/ shows
the bucket contents.

## Notes

- After job cleanup an empty `__magic/` directory can still be listed for a
  couple of minutes: SeaweedFS removes empty folders asynchronously. The job
  prints a note when it sees one, and only fails if files remain inside.
- `spark-hadoop-cloud` is published for Spark 3.5.x on Maven Central. Keep its
  version in step with Spark, and `hadoop-aws` in step with the Hadoop client
  bundled in Spark (3.3.4 for Spark 3.5.1).
