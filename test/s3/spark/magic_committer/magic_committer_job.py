"""Write to SeaweedFS with the S3A magic committer and verify the result.

Runs inside the spark container, which already carries the S3A and magic
committer settings in spark-defaults.conf:

    spark-submit /opt/jobs/magic_committer_job.py [--target URI] [--rows N] [--tasks N]

Exits non-zero if any check fails.
"""

import argparse
import json
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

failures = []


def check(ok, message):
    print(("  PASS  " if ok else "  FAIL  ") + message)
    if not ok:
        failures.append(message)


def read_text(spark, fs, path):
    stream = fs.open(path)
    try:
        return spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()


def verify_commit(spark, fs, target, label):
    jvm = spark._jvm
    Path = jvm.org.apache.hadoop.fs.Path
    print("\n== " + label + " ==")

    # A FileOutputCommitter writes an empty _SUCCESS; S3A committers write a
    # JSON manifest naming the committer and every file it committed.
    success_path = Path(target + "/_SUCCESS")
    if not fs.exists(success_path):
        check(False, "_SUCCESS exists")
        return
    raw = read_text(spark, fs, success_path)
    try:
        success = json.loads(raw)
    except ValueError:
        check(False, "_SUCCESS is an S3A committer manifest (got %d bytes of non-JSON)" % len(raw))
        return
    check(success.get("committer") == "magic",
          "_SUCCESS written by the magic committer (committer=%r)" % success.get("committer"))

    filenames = success.get("filenames") or []
    check(len(filenames) > 0, "_SUCCESS lists %d committed files" % len(filenames))
    store = fs.getUri().toString()
    empty, missing, total_bytes, max_parts = [], [], 0, 0
    for name in filenames:
        path = Path(store + name)
        if not fs.exists(path):
            missing.append(name)
            continue
        status = fs.getFileStatus(path)
        total_bytes += status.getLen()
        if status.getLen() == 0:
            empty.append(name)
        # A completed multipart upload has an ETag ending in -<part count>.
        etag = (status.getETag() or "").strip('"')
        max_parts = max(max_parts, int(etag.rsplit("-", 1)[1]) if "-" in etag else 1)
    check(not missing, "every committed file exists" + (": missing %s" % missing if missing else ""))
    check(not empty, "no committed file is empty" + (": empty %s" % empty if empty else ""))
    print("        committed %d bytes in %d files, up to %d multipart parts per file"
          % (total_bytes, len(filenames), max_parts))

    # The manifest carries the filesystem's counters, so they add up across
    # the writes of this session.
    metrics = success.get("metrics") or {}
    for key in sorted(metrics):
        if key.startswith("committer_") and metrics[key]:
            print("        %s = %s" % (key, metrics[key]))

    # Job cleanup deletes __magic and its .pending/.pendingset files.
    leftovers = []
    for status in fs.listStatus(Path(target)):
        name = status.getPath().getName()
        if not name.startswith("__magic"):
            continue
        files = fs.listFiles(status.getPath(), True)
        while files.hasNext():
            leftovers.append(files.next().getPath().toString())
        if not leftovers:
            print("        note: empty %s/ still listed; SeaweedFS removes empty folders asynchronously" % name)
    check(not leftovers, "no files left under __magic" + (": %s" % leftovers if leftovers else ""))

    # Uploads are completed at job commit; one left open is leaked data.
    prefix = Path(target).toUri().getPath().lstrip("/") + "/"
    pending = []
    uploads = fs.listUploads(prefix)
    while uploads.hasNext():
        pending.append(uploads.next().getKey())
    check(not pending, "no pending multipart uploads under %s" % prefix
          + (": %s" % pending if pending else ""))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="s3a://spark/magic-committer/output")
    parser.add_argument("--rows", type=int, default=3000000)
    parser.add_argument("--tasks", type=int, default=4)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("seaweedfs-s3a-magic-committer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.Path(args.target).getFileSystem(hadoop_conf)

    print("\nfs.s3a.endpoint                = %s" % hadoop_conf.get("fs.s3a.endpoint"))
    print("fs.s3a.committer.name          = %s" % hadoop_conf.get("fs.s3a.committer.name"))
    print("fs.s3a.committer.magic.enabled = %s" % hadoop_conf.get("fs.s3a.committer.magic.enabled"))
    print("target                         = %s" % args.target)

    # sha2 hex strings barely compress, so each file spans several 5M parts.
    df = (spark.range(0, args.rows, numPartitions=args.tasks)
          .withColumn("part", F.col("id") % 4)
          .withColumn("payload", F.sha2(F.col("id").cast("string"), 256)))

    df.write.mode("overwrite").partitionBy("part").parquet(args.target)
    verify_commit(spark, fs, args.target, "overwrite")

    df.write.mode("append").partitionBy("part").parquet(args.target)
    verify_commit(spark, fs, args.target, "append")

    print("\n== read back ==")
    expected = 2 * args.rows
    result = spark.read.parquet(args.target)
    count = result.count()
    check(count == expected, "row count %d == %d" % (count, expected))
    distinct = result.select("id").distinct().count()
    check(distinct == args.rows, "distinct ids %d == %d" % (distinct, args.rows))

    spark.stop()
    if failures:
        print("\nMAGIC COMMITTER TEST FAILED: %d check(s) failed" % len(failures))
        sys.exit(1)
    print("\nMAGIC COMMITTER TEST PASSED")


if __name__ == "__main__":
    main()
