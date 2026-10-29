package s3tables

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedRetainedTable drops ns.t the way a dropTable without purgeRequested
// does, after giving its directory real files, and returns those files as
// dir/name paths.
func seedRetainedTable(t *testing.T, fs *s3tablestest.MemFiler, m *Manager) []string {
	t.Helper()
	tablePath := GetTablePath(renameTestBucket, "ns", "t")
	retained := []string{
		path.Join(tablePath, "metadata", "v3.metadata.json"),
		path.Join(tablePath, "data", "00000-0-data.parquet"),
	}
	for _, file := range retained {
		fs.PutFile(path.Dir(file), path.Base(file), time.Now())
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "t",
		RetainData:     true,
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)
	return retained
}

func assertFilesKept(t *testing.T, fs *s3tablestest.MemFiler, files []string) {
	t.Helper()
	for _, file := range files {
		entry := fs.Get(path.Dir(file), path.Base(file))
		if assert.NotNilf(t, entry, "%s must be kept", file) {
			assert.Falsef(t, entry.IsDirectory, "%s must still be a file", file)
		}
	}
}

func listNamespaceNames(t *testing.T, m *Manager, fs *s3tablestest.MemFiler) []string {
	t.Helper()
	var resp ListNamespacesResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListNamespaces", &ListNamespacesRequest{
		TableBucketARN: mustBucketARN(t),
	}, &resp, ""))
	var names []string
	for _, ns := range resp.Namespaces {
		names = append(names, strings.Join(ns.Namespace, "."))
	}
	return names
}

func deleteNamespace(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace string) (int, S3TablesError) {
	t.Helper()
	return runHandler(t, m, fs.Client, "DeleteNamespace", &DeleteNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
	})
}

// Dropping a namespace whose dropped table kept its files removes the
// namespace from the catalog and leaves the files where they were, ready for
// a register_table.
func TestDeleteNamespaceKeepsRetainedFiles(t *testing.T) {
	fs, m := startRenameManager(t)
	retained := seedRetainedTable(t, fs, m)

	status, resp := deleteNamespace(t, m, fs, "ns")
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	status, resp = runHandler(t, m, fs.Client, "GetNamespace", &GetNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	})
	assert.Equal(t, http.StatusNotFound, status, "the namespace must be gone from the catalog")
	assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type)
	assert.NotContains(t, listNamespaceNames(t, m, fs), "ns")

	nsEntry := fs.Get(GetTableBucketPath(renameTestBucket), "ns")
	require.NotNil(t, nsEntry, "the namespace directory must stay to hold the retained files")
	for _, key := range s3tablesExtendedKeys {
		assert.NotContainsf(t, nsEntry.Extended, key, "catalog attribute %s must be cleared", key)
	}
	assertFilesKept(t, fs, retained)
}

// Directories a rename or a drop left behind without any file in them do not
// hold anything worth keeping, so the namespace goes with them as before.
func TestDeleteNamespaceRemovesEmptyLeftovers(t *testing.T) {
	fs, m := startRenameManager(t)
	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "t",
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.Put(nsPath, "left", nil)
	fs.Put(path.Join(nsPath, "left"), "data", nil)
	fs.Put(path.Join(nsPath, "left", "data"), "part=1", nil)

	status, resp = deleteNamespace(t, m, fs, "ns")
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	assert.Nil(t, fs.Get(GetTableBucketPath(renameTestBucket), "ns"), "the namespace directory must be deleted")
	assert.Nil(t, fs.Get(path.Join(nsPath, "left"), "data"), "empty leftovers go with the namespace")
}

// The live table sorts after twelve leftover directories, past the first ten
// entries the emptiness check used to read: it must still block the delete.
func TestDeleteNamespaceSeesEveryChild(t *testing.T) {
	fs, m := startRenameManager(t)
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	for i := 0; i < 12; i++ {
		fs.Put(nsPath, fmt.Sprintf("a%02d", i), nil)
	}

	status, resp := deleteNamespace(t, m, fs, "ns")
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeNamespaceNotEmpty, resp.Type)

	got, err := runGetTable(t, m, fs, "ns", "t")
	require.NoError(t, err, "the live table must survive")
	assert.Equal(t, "t", got.Name)
	assert.NotNil(t, fs.Get(path.Join(GetTablePath(renameTestBucket, "ns", "t"), "metadata"), "v3.metadata.json"))
}

// Creating a dropped namespace again takes back the directory its retained
// files sit in. The files stay, and the dropped table stays unregistered.
func TestCreateNamespaceAdoptsRetainedDirectory(t *testing.T) {
	fs, m := startRenameManager(t)
	retained := seedRetainedTable(t, fs, m)
	status, resp := deleteNamespace(t, m, fs, "ns")
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	status, resp = runHandler(t, m, fs.Client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Properties:     map[string]string{"owner": "etl"},
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	var got GetNamespaceResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "GetNamespace", &GetNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
	}, &got, ""))
	assert.Equal(t, []string{"ns"}, got.Namespace)
	assert.Equal(t, "etl", got.Properties["owner"])
	assert.Contains(t, listNamespaceNames(t, m, fs), "ns")

	assertFilesKept(t, fs, retained)
	status, resp = runHandler(t, m, fs.Client, "GetTable", &GetTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "t",
	})
	assert.Equal(t, http.StatusNotFound, status, "the dropped table must stay unregistered")
	assert.Equal(t, ErrCodeNoSuchTable, resp.Type)
	assert.NotContains(t, listTableNames(t, m, fs, "ns"), "t")
}

// Only a directory without a catalog record can be adopted: a live namespace
// or a file at the namespace's path is a name conflict, and neither is touched.
func TestAdoptNamespaceDirectoryRefusesTakenNames(t *testing.T) {
	fs, m := startRenameManager(t)
	bucketPath := GetTableBucketPath(renameTestBucket)
	fs.PutFile(bucketPath, "notns", time.Now())
	liveRecord := fs.Get(bucketPath, "ns").Extended[ExtendedKeyMetadata]
	attrs := map[string][]byte{ExtendedKeyMetadata: []byte(`{"namespace":["x"]}`)}

	for _, tc := range []struct {
		name      string
		namespace string
	}{
		{"a live namespace", "ns"},
		{"a file at the path", "notns"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := m.handler.adoptNamespaceDirectory(context.Background(), fs.Client, GetNamespacePath(renameTestBucket, tc.namespace), attrs)
			assert.ErrorIs(t, err, ErrNameTaken)
		})
	}

	assert.Equal(t, liveRecord, fs.Get(bucketPath, "ns").Extended[ExtendedKeyMetadata], "the live namespace's record must be untouched")
	assert.Empty(t, fs.Get(bucketPath, "notns").Extended, "the file must not get a catalog record")

	status, resp := runHandler(t, m, fs.Client, "CreateNamespace", &CreateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"notns"},
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeNamespaceAlreadyExists, resp.Type)
}

// A dropped table or namespace leaves its directory without a catalog record.
// Every operation on such a name answers not found, never an internal error.
func TestRetainedNamesReportNotFound(t *testing.T) {
	fs, m := startRenameManager(t)
	seedRetainedTable(t, fs, m)
	arn := mustBucketARN(t)
	ns := []string{"ns"}

	for _, tc := range []struct {
		operation string
		input     interface{}
	}{
		{"GetTable", &GetTableRequest{TableBucketARN: arn, Namespace: ns, Name: "t"}},
		{"DeleteTable", &DeleteTableRequest{TableBucketARN: arn, Namespace: ns, Name: "t"}},
		{"UpdateTable", &UpdateTableRequest{TableBucketARN: arn, Namespace: ns, Name: "t",
			MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v4.metadata.json"}},
		{"PutTablePolicy", &PutTablePolicyRequest{TableBucketARN: arn, Namespace: ns, Name: "t",
			ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`}},
		{"GetTablePolicy", &GetTablePolicyRequest{TableBucketARN: arn, Namespace: ns, Name: "t"}},
		{"DeleteTablePolicy", &DeleteTablePolicyRequest{TableBucketARN: arn, Namespace: ns, Name: "t"}},
		{"GetTableMaintenanceConfiguration", &GetTableMaintenanceConfigurationRequest{TableBucketARN: arn, Namespace: ns, Name: "t"}},
	} {
		t.Run("dropped table "+tc.operation, func(t *testing.T) {
			status, resp := runHandler(t, m, fs.Client, tc.operation, tc.input)
			assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchTable, resp.Type)
		})
	}

	status, resp := deleteNamespace(t, m, fs, "ns")
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	for _, tc := range []struct {
		operation string
		input     interface{}
	}{
		{"GetNamespace", &GetNamespaceRequest{TableBucketARN: arn, Namespace: ns}},
		{"UpdateNamespace", &UpdateNamespaceRequest{TableBucketARN: arn, Namespace: ns, Properties: map[string]string{"k": "v"}}},
		{"DeleteNamespace", &DeleteNamespaceRequest{TableBucketARN: arn, Namespace: ns}},
	} {
		t.Run("dropped namespace "+tc.operation, func(t *testing.T) {
			status, resp := runHandler(t, m, fs.Client, tc.operation, tc.input)
			assert.Equal(t, http.StatusNotFound, status, "%+v", resp)
			assert.Equal(t, ErrCodeNoSuchNamespace, resp.Type)
		})
	}
	// ListTables answers a missing namespace with an empty listing, and a
	// dropped one the same way.
	assert.Empty(t, listTableNames(t, m, fs, "ns"))
}
