package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listTableNames(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace string) []string {
	t.Helper()
	var resp ListTablesResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTables", &ListTablesRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
	}, &resp, ""))
	var names []string
	for _, table := range resp.Tables {
		names = append(names, table.Name)
	}
	return names
}

func assertCatalogRecordCleared(t *testing.T, fs *s3tablestest.MemFiler, namespace, name string) {
	t.Helper()
	entry := fs.Get(GetNamespacePath(renameTestBucket, namespace), name)
	require.NotNil(t, entry, "the table directory must survive")
	for _, key := range renamedTableAttributes {
		_, present := entry.Extended[key]
		assert.Falsef(t, present, "catalog attribute %s must be cleared", key)
	}
}

// A delete without RetainData keeps the purging behavior AWS defines; with it,
// only the catalog record goes and every file under the table stays, so the
// table can be registered again from its last metadata file.
func TestDeleteTableRetainData(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retainData bool
	}{
		{"default delete purges the table's files", false},
		{"RetainData keeps the table's files", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, m := startRenameManager(t)
			tablePath := GetTablePath(renameTestBucket, "ns", "t")

			status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns"},
				Name:           "t",
				RetainData:     tc.retainData,
			})
			require.Equal(t, http.StatusOK, status, "%+v", resp)

			status, resp = runHandler(t, m, fs.Client, "GetTable", &GetTableRequest{
				TableBucketARN: mustBucketARN(t),
				Namespace:      []string{"ns"},
				Name:           "t",
			})
			assert.Equal(t, http.StatusNotFound, status, "the table must be gone from the catalog")
			assert.Equal(t, ErrCodeNoSuchTable, resp.Type)
			assert.NotContains(t, listTableNames(t, m, fs, "ns"), "t")

			metadataFile := fs.Get(path.Join(tablePath, "metadata"), "v3.metadata.json")
			dataDir := fs.Get(tablePath, "data")
			if tc.retainData {
				assert.NotNil(t, metadataFile, "the last metadata file must be kept")
				assert.NotNil(t, dataDir, "the data directory must be kept")
				assertCatalogRecordCleared(t, fs, "ns", "t")
			} else {
				assert.Nil(t, metadataFile, "a purging delete removes the metadata file")
				assert.Nil(t, dataDir, "a purging delete removes the data")
				assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "a purging delete removes the table directory")
			}
		})
	}
}

// A decoupled table keeps its data away from its name path (here ns/newt-x
// while the record sits on ns/newt, which holds another table's leftover).
// RetainData must leave both directories alone and only clear the record.
func TestDeleteTableRetainDataKeepsDecoupledLocation(t *testing.T) {
	fs, m := startRenameManager(t)

	meta, _ := json.Marshal(tableMetadataInternal{
		Name:             "newt",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/newt-x/metadata/v1.metadata.json",
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "newt", map[string][]byte{
		ExtendedKeyMetadata:  meta,
		ExtendedKeyTags:      []byte(`{"k":"v"}`),
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})
	fs.Put(GetTablePath(renameTestBucket, "ns", "newt"), "leftover", nil)
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "newt-x", nil)
	fs.Put(GetTablePath(renameTestBucket, "ns", "newt-x"), "metadata", nil)
	fs.Put(path.Join(GetTablePath(renameTestBucket, "ns", "newt-x"), "metadata"), "v1.metadata.json", nil)

	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "newt",
		RetainData:     true,
	})
	require.Equal(t, http.StatusOK, status, "%+v", resp)

	assert.NotNil(t, fs.Get(path.Join(GetTablePath(renameTestBucket, "ns", "newt-x"), "metadata"), "v1.metadata.json"),
		"the decoupled data location must be kept")
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "newt"), "leftover"),
		"data under the name path must be kept")
	assertCatalogRecordCleared(t, fs, "ns", "newt")
}

// A token delete removes the record only while it still holds what the caller
// read: a write that lands in between turns it into a conflict, and the
// updated record survives instead of being dropped with the old one.
func TestDeleteTableRetainDataTokenLosesToConcurrentUpdate(t *testing.T) {
	fs, m := startRenameManager(t)

	token := "tok-rt"
	meta, _ := json.Marshal(tableMetadataInternal{
		Name:           "rt",
		Namespace:      "ns",
		Format:         "ICEBERG",
		OwnerAccountID: DefaultAccountID,
		VersionToken:   token,
	})
	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.Put(nsPath, "rt", map[string][]byte{ExtendedKeyMetadata: meta})
	fs.Put(GetTablePath(renameTestBucket, "ns", "rt"), "data", nil)

	fs.BeforeUpdate = func() {
		fs.Put(nsPath, "rt", map[string][]byte{
			ExtendedKeyMetadata: meta,
			ExtendedKeyTags:     []byte(`{"k":"v"}`),
		})
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "rt",
		VersionToken:   token,
		RetainData:     true,
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeConflict, resp.Type)

	entry := fs.Get(nsPath, "rt")
	require.NotNil(t, entry)
	assert.Equal(t, meta, entry.Extended[ExtendedKeyMetadata], "the updated record must survive")
	assert.Equal(t, []byte(`{"k":"v"}`), entry.Extended[ExtendedKeyTags])
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "rt"), "data"))
}
