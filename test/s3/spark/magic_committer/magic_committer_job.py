"""Write to SeaweedFS with the S3A magic committer and verify the result.

Runs inside the spark container, which already carries the S3A and magic
committer settings in spark-defaults.conf:

    spark-submit /opt/jobs/magic_committer_job.py [--target URI] [--rows N] [--tasks N]
        [--inject task-error|executor-kill]

--inject makes the first attempt of some tasks die part-way through writing,
while their multipart uploads have parts in flight, so the retried attempt has
to commit on top of what the failed one left behind:

  task-error     the task raises; Spark aborts it and retries it. Needs local
                 mode with retries, e.g. local[4,4], so the driver JVM also sees
                 the task's aborts.
  executor-kill  the task SIGKILLs its executor JVM, so no abort runs at all and
                 job cleanup has to find the orphaned uploads. Needs separate
                 executor processes, e.g. local-cluster[2,2,1024].

Exits non-zero if any check fails.
"""

import argparse
import json
import os
import shutil
import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Failed attempts drop a marker here; driver and executors share the container.
INJECT_DIR = "/tmp/magic-committer-inject"

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


def payload_udf(inject, victims, fail_after):
    """sha256 of the id, as F.sha2 would compute it, in Python so that the first
    attempt of each victim task can fail after fail_after rows."""

    def kill_executor():
        # The Python worker runs under the executor JVM, possibly via the
        # pyspark daemon; walk up to it rather than trusting a fixed depth.
        pid = os.getpid()
        for _ in range(4):
            with open("/proc/%d/stat" % pid) as f:
                pid = int(f.read().rsplit(")", 1)[1].split()[1])
            with open("/proc/%d/cmdline" % pid, "rb") as f:
                if b"CoarseGrainedExecutorBackend" in f.read():
                    os.kill(pid, 9)
                    os._exit(1)
        raise RuntimeError("executor JVM not found above pid %d" % os.getpid())

    seen = {}

    def payload(i):
        import hashlib
        from pyspark import TaskContext

        ctx = TaskContext.get()
        if ctx.attemptNumber() == 0 and ctx.partitionId() in victims:
            key = (ctx.stageId(), ctx.partitionId())
            seen[key] = seen.get(key, 0) + 1
            if seen[key] >= fail_after:
                os.makedirs(INJECT_DIR, exist_ok=True)
                with open("%s/stage%d-task%d" % (INJECT_DIR, key[0], key[1]), "w") as f:
                    f.write("%s after %d rows\n" % (inject, seen[key]))
                if inject == "executor-kill":
                    kill_executor()
                raise RuntimeError("injected failure in task %d attempt 0" % key[1])
        return hashlib.sha256(str(i).encode()).hexdigest()

    return F.udf(payload, "string")


def list_data_files(spark, fs, target):
    files = set()
    entries = fs.listFiles(spark._jvm.org.apache.hadoop.fs.Path(target), True)
    while entries.hasNext():
        path = entries.next().getPath()
        if "/__magic" in path.toUri().getPath() or path.getName().startswith(("_", ".")):
            continue
        files.add(path.toUri().getPath())
    return files


def verify_commit(spark, fs, target, visible_before):
    """Checks one write and returns the data files visible after it."""
    jvm = spark._jvm
    Path = jvm.org.apache.hadoop.fs.Path

    # A FileOutputCommitter writes an empty _SUCCESS; S3A committers write a
    # JSON manifest naming the committer and every file it committed.
    success_path = Path(target + "/_SUCCESS")
    if not fs.exists(success_path):
        check(False, "_SUCCESS exists")
        return set()
    raw = read_text(spark, fs, success_path)
    try:
        success = json.loads(raw)
    except ValueError:
        check(False, "_SUCCESS is an S3A committer manifest (got %d bytes of non-JSON)" % len(raw))
        return set()
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

    # Nothing a failed attempt wrote may show up: the table is what was there
    # before plus exactly what this job committed.
    visible = list_data_files(spark, fs, target)
    stray = sorted(visible - visible_before - set(filenames))
    check(not stray, "only committed files are visible" + (": stray %s" % stray if stray else ""))

    # The manifest carries the filesystem's counters, so they add up across
    # the writes of this session.
    metrics = success.get("metrics") or {}
    for key in sorted(metrics):
        if key.startswith(("committer_", "object_multipart")) and metrics[key]:
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
    return visible


def aborted_uploads(fs):
    value = fs.getIOStatistics().counters().get("object_multipart_aborted")
    return 0 if value is None else int(value)


def write(fs, df, target, mode, inject, victims):
    print("\n== " + mode + " ==")
    shutil.rmtree(INJECT_DIR, ignore_errors=True)
    aborted_before = aborted_uploads(fs)
    writer = df.write.mode(mode).partitionBy("part")
    if inject != "none":
        # 1 MiB row groups reach the output stream as they fill, so the failure
        # lands while each open file has multipart parts uploaded.
        writer = writer.option("parquet.block.size", 1 << 20)
    writer.parquet(target)
    if inject != "none":
        injected = sorted(os.listdir(INJECT_DIR)) if os.path.isdir(INJECT_DIR) else []
        check(len(injected) == len(victims),
              "%d task attempt(s) failed by %s and were retried: %s"
              % (len(injected), inject, ", ".join(injected) or "none"))
        # Counted in this JVM: the task abort under task-error (local mode), the
        # job cleanup's sweep of orphaned uploads under executor-kill.
        aborted = aborted_uploads(fs) - aborted_before
        check(aborted > 0, "%d multipart upload(s) left by failed attempts were aborted" % aborted)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="s3a://spark/magic-committer/output")
    parser.add_argument("--rows", type=int, default=3000000)
    parser.add_argument("--tasks", type=int, default=4)
    parser.add_argument("--inject", choices=["none", "task-error", "executor-kill"], default="none")
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
    print("inject                         = %s" % args.inject)

    df = (spark.range(0, args.rows, numPartitions=args.tasks)
          .withColumn("part", F.col("id") % 4))
    # sha2 hex strings barely compress, so each file spans several 5M parts.
    victims = set()
    if args.inject == "none":
        df = df.withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
    else:
        # Killing an executor also takes down the other task running on it.
        victims = {0} if args.inject == "executor-kill" else {0, 2}
        fail_after = int(args.rows / args.tasks * 0.6)
        df = df.withColumn("payload", payload_udf(args.inject, victims, fail_after)(F.col("id")))
        # Stream rows into one open file per partition instead of sorting the
        # whole task first, so the failure hits files that are mid-upload.
        spark.conf.set("spark.sql.optimizer.plannedWrite.enabled", "false")
        spark.conf.set("spark.sql.maxConcurrentOutputFileWriters", "8")

    write(fs, df, args.target, "overwrite", args.inject, victims)
    visible = verify_commit(spark, fs, args.target, set())

    write(fs, df, args.target, "append", args.inject, victims)
    verify_commit(spark, fs, args.target, visible)

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
