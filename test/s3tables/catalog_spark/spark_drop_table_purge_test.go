package catalog_spark

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// TestSparkDropTableKeepsFilesWithoutPurge checks the two drops Spark sends.
// DROP TABLE carries no purgeRequested, which the REST spec defaults to false:
// the table leaves the catalog but its data and metadata files stay, so it can
// be registered again from its last metadata file. DROP TABLE ... PURGE sends
// purgeRequested=true and must leave nothing behind.
func TestSparkDropTableKeepsFilesWithoutPurge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, tableBucket := setupSparkTestEnv(t)

	namespace := "drop_purge_" + randomString(6)
	table := "t_" + randomString(6)
	ident := fmt.Sprintf("iceberg.%s.%s", namespace, table)

	output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%[1]s")
spark.sql("CREATE TABLE %[2]s (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO %[2]s VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
meta = spark.sql("SELECT file FROM %[2]s.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()[0]['file']
print(f"metadata file: {meta}")
spark.sql("DROP TABLE %[2]s")
print("dropped without purge")
`, namespace, ident), env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "dropped without purge") {
		t.Fatalf("create/insert/drop failed, output:\n%s", output)
	}
	match := regexp.MustCompile(`metadata file: (\S+)`).FindStringSubmatch(output)
	if match == nil {
		t.Fatalf("no metadata file in output:\n%s", output)
	}
	metadataFile := match[1]

	prefix := namespace + "/" + table + "/"
	keys := listSparkBucketKeysUnder(t, env, tableBucket, prefix)
	for _, suffix := range []string{".metadata.json", ".parquet"} {
		if !hasKeyWithSuffix(keys, suffix) {
			t.Fatalf("DROP TABLE without PURGE deleted the %s files under %s; keys left: %v", suffix, prefix, keys)
		}
	}

	output = runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CALL iceberg.system.register_table('%[1]s.%[2]s', '%[3]s')")
count = spark.sql("SELECT COUNT(*) AS c FROM %[4]s").collect()[0]['c']
print(f"restored row count: {count}")
`, namespace, table, metadataFile, ident), env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "restored row count: 3") {
		t.Fatalf("register_table from the kept metadata file did not restore the table, output:\n%s", output)
	}

	output = runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("DROP TABLE %s PURGE")
print("dropped with purge")
`, ident), env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "dropped with purge") {
		t.Fatalf("DROP TABLE ... PURGE failed, output:\n%s", output)
	}
	if keys := listSparkBucketKeysUnder(t, env, tableBucket, prefix); len(keys) != 0 {
		t.Fatalf("DROP TABLE ... PURGE left files under %s: %v", prefix, keys)
	}
}

func hasKeyWithSuffix(keys []string, suffix string) bool {
	for _, k := range keys {
		if strings.HasSuffix(k, suffix) {
			return true
		}
	}
	return false
}
