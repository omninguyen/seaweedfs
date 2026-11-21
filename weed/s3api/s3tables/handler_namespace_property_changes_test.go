package s3tables

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Iceberg catalog now sends its changes as PropertyChanges. That write
// must keep what the whole-map write keeps: the namespace's owner and creation
// time, and the properties it was not asked to change.
func TestUpdateNamespacePropertyChangesEditsInPlace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	requireOK(t, m, fs, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Properties:     map[string]string{"keep": "1", "drop": "1"},
	})
	before := namespaceMetadataOf(t, fs, "ns")

	code, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN:  mustBucketARN(t),
		Namespace:       []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"edited": "true"}, Removals: []string{"drop", "absent"}},
	})

	require.Equal(t, http.StatusOK, code, "%+v", resp)
	after := namespaceMetadataOf(t, fs, "ns")
	assert.Equal(t, map[string]string{"keep": "1", "edited": "true"}, after.Properties)
	assert.True(t, after.CreatedAt.Equal(before.CreatedAt), "an update must keep the creation time: %v became %v", before.CreatedAt, after.CreatedAt)
	assert.Equal(t, before.OwnerAccountID, after.OwnerAccountID)
}

// TestUpdateNamespaceSparesARecreatedNamespace for the PropertyChanges form:
// a namespace deleted and created again before the write is refused, not
// edited with the first one's owner.
func TestUpdateNamespacePropertyChangesSparesARecreatedNamespace(t *testing.T) {
	fs, m := startEmptyBucket(t)
	requireOK(t, m, fs, "CreateNamespace", createNamespaceNsRequest(t))
	bucketPath := GetTableBucketPath(renameTestBucket)
	recreated := recreatedNamespace(t, "ns")
	fs.OnUpdate = func(dir, name string) {
		if dir != bucketPath || name != "ns" {
			return
		}
		fs.OnUpdate = nil
		fs.Put(bucketPath, "ns", recreated)
	}

	code, resp := runHandler(t, m, fs.Client, "UpdateNamespace", &UpdateNamespaceRequest{
		TableBucketARN:  mustBucketARN(t),
		Namespace:       []string{"ns"},
		PropertyChanges: &NamespacePropertyChanges{Updates: map[string]string{"edited": "true"}},
	})

	assert.Equal(t, http.StatusConflict, code, "%+v", resp)
	assert.Equal(t, recreated, fs.Get(bucketPath, "ns").Extended, "the new namespace is untouched")
}
