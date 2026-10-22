package spark

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// magicCommitterConf routes Spark SQL writes through the S3A magic committer.
// Tasks stream their output as multipart uploads that are only completed at
// job commit, so the object store never sees a rename.
var magicCommitterConf = map[string]string{
	// Match hadoop-client 3.3.4 shipped in apache/spark:3.5.1.
	"spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-hadoop-cloud_2.12:3.5.1",
	// spark-hadoop-cloud drags in every cloud connector; only its commit
	// protocol classes are needed, the rest is already in the Spark image.
	"spark.jars.excludes": strings.Join([]string{
		"com.google.cloud.bigdataoss:gcs-connector",
		"org.apache.hadoop:hadoop-azure",
		"org.apache.hadoop:hadoop-cloud-storage",
		"org.apache.hadoop:hadoop-client-api",
		"org.apache.hadoop:hadoop-client-runtime",
		"joda-time:joda-time",
		"com.fasterxml.jackson.core:jackson-databind",
		"com.fasterxml.jackson.core:jackson-annotations",
		"com.fasterxml.jackson.dataformat:jackson-dataformat-cbor",
		"org.apache.httpcomponents:httpclient",
		"org.apache.httpcomponents:httpcore",
		"org.eclipse.jetty:jetty-util",
		"org.eclipse.jetty:jetty-util-ajax",
	}, ","),
	"spark.sql.sources.commitProtocolClass":                     "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
	"spark.sql.parquet.output.committer.class":                  "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
	"spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
	"spark.hadoop.fs.s3a.committer.name":                        "magic",
	"spark.hadoop.fs.s3a.committer.magic.enabled":               "true",
}

// s3aSuccessData is the subset of the JSON _SUCCESS manifest that S3A
// committers write (org.apache.hadoop.fs.s3a.commit.files.SuccessData).
type s3aSuccessData struct {
	Committer string   `json:"committer"`
	Filenames []string `json:"filenames"`
}

func TestSparkS3AMagicCommitter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Spark integration test in short mode")
	}

	env := setupSparkIssue8234Env(t)

	script := `
import pyspark.sql.functions as F

target = "s3a://test/magic-committer/output"

df = spark.range(0, 1000).repartition(8).withColumn("part", F.col("id") % 4)
df.write.format("parquet").mode("overwrite").partitionBy("part").save(target)
print("OVERWRITE_OK")
df.write.format("parquet").mode("append").partitionBy("part").save(target)
print("APPEND_OK")

count = spark.read.parquet(target).count()
print("WRITE_COUNT=" + str(count))
`

	code, output := runSparkPyScriptWithConf(t, env.sparkContainer, script, env.s3Port, magicCommitterConf)
	if code != 0 {
		t.Fatalf("Spark script exited with code %d; output:\n%s", code, output)
	}
	if !strings.Contains(output, "WRITE_COUNT=2000") {
		t.Fatalf("expected 2000 rows after overwrite + append, got:\n%s", output)
	}

	// A FileOutputCommitter writes an empty _SUCCESS; only S3A committers
	// write a JSON manifest, so this proves the magic committer ran.
	success := readS3ASuccessData(t, env, "test", "magic-committer/output/_SUCCESS")
	if success.Committer != "magic" {
		t.Fatalf("expected _SUCCESS written by the magic committer, got committer %q", success.Committer)
	}
	if len(success.Filenames) == 0 {
		t.Fatalf("_SUCCESS manifest lists no committed files")
	}

	client := newS3Client(env)
	sizes := make(map[string]int64)
	for _, object := range listObjectsByPrefix(t, env, "test", "magic-committer/") {
		sizes[aws.ToString(object.Key)] = aws.ToInt64(object.Size)
	}

	// Every file the job committed must be a completed upload with data,
	// not a zero-byte magic marker.
	for _, filename := range success.Filenames {
		key := strings.TrimPrefix(filename, "/")
		size, ok := sizes[key]
		if !ok {
			t.Fatalf("committed file %s is missing from the bucket; keys: %v", key, mapKeys(sizes))
		}
		if size == 0 {
			t.Fatalf("committed file %s is empty", key)
		}
	}

	var magicKeys []string
	for key := range sizes {
		if hasMagicPathSegment(key) && !strings.HasSuffix(key, "/") {
			magicKeys = append(magicKeys, key)
		}
	}
	if len(magicKeys) > 0 {
		t.Fatalf("magic committer left files under __magic: %v", magicKeys)
	}
	// Empty folder cleanup has a 2m default delay + 30s processor interval
	if lingering := waitForObjectsToDisappear(t, env, "test", []string{"magic-committer/output/__magic/"}, 3*time.Minute); len(lingering) > 0 {
		t.Fatalf("magic committer left its __magic directory behind: %v", lingering)
	}

	uploads, err := client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String("test"),
		Prefix: aws.String("magic-committer/"),
	})
	if err != nil {
		t.Fatalf("failed to list multipart uploads: %v", err)
	}
	if len(uploads.Uploads) > 0 {
		var pending []string
		for _, upload := range uploads.Uploads {
			pending = append(pending, aws.ToString(upload.Key))
		}
		t.Fatalf("magic committer left uncompleted multipart uploads: %v", pending)
	}
}

func readS3ASuccessData(t *testing.T, env *TestEnvironment, bucketName, key string) s3aSuccessData {
	t.Helper()

	output, err := newS3Client(env).GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("failed to get %s: %v", key, err)
	}
	defer output.Body.Close()

	body, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("failed to read %s: %v", key, err)
	}
	var success s3aSuccessData
	if err := json.Unmarshal(body, &success); err != nil {
		t.Fatalf("%s is not an S3A committer manifest (%v): %q", key, err, body)
	}
	return success
}

func hasMagicPathSegment(key string) bool {
	for _, segment := range strings.Split(strings.TrimSuffix(key, "/"), "/") {
		if strings.HasPrefix(segment, "__magic") {
			return true
		}
	}
	return false
}

func mapKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
