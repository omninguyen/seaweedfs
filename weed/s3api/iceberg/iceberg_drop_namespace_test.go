package iceberg

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// seedOwnedBucket marks the bucket seedNamespace creates as a table bucket
// owned by owner, which CreateNamespace requires of its caller.
func seedOwnedBucket(fc *memFiler, bucket, owner string) {
	meta, _ := json.Marshal(map[string]any{"name": bucket, "ownerAccountId": owner})
	fc.seed(s3tables.GetTableBucketPath(bucket), &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3tables.ExtendedKeyTableBucket: []byte("{}"),
			s3tables.ExtendedKeyMetadata:    meta,
		},
	})
}

func newNamespaceRequest(t *testing.T, method, namespace string, body any) *http.Request {
	t.Helper()
	target := "/v1/" + dropTestBucket + "/namespaces"
	vars := map[string]string{"prefix": dropTestBucket}
	if namespace != "" {
		target += "/" + namespace
		vars["namespace"] = namespace
	}
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, target, bytes.NewReader(payload))
	r = mux.SetURLVars(r, vars)
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
}

// A table dropped without purge keeps its files through a drop of its
// namespace, so it can still be registered again. Creating the namespace and
// the table again leaves those files alone: the new table gets a fresh
// location.
func TestDropNamespaceKeepsFilesOfDroppedTables(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, dropTestBucket, "finance", "alice")
	seedOwnedBucket(fc, dropTestBucket, "alice")
	s := NewServer(fc, nil)
	oldDir := createTableWithData(t, s, fc, "events")
	retained := filesUnder(fc, oldDir)
	slices.Sort(retained)
	if len(retained) < 2 {
		t.Fatalf("setup: want a metadata and a data file under %s, have %v", oldDir, retained)
	}

	w := httptest.NewRecorder()
	s.handleDropTable(w, newTableRequest(t, http.MethodDelete, "finance", "events", ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("drop table status = %d (body: %s)", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	s.handleDropNamespace(w, newNamespaceRequest(t, http.MethodDelete, "finance", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("drop namespace status = %d (body: %s)", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	s.handleNamespaceExists(w, newNamespaceRequest(t, http.MethodHead, "finance", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("namespace exists after drop = %d, want %d", w.Code, http.StatusNotFound)
	}
	kept := filesUnder(fc, oldDir)
	slices.Sort(kept)
	if !slices.Equal(kept, retained) {
		t.Fatalf("after DROP NAMESPACE: files under %s = %v, want the retained %v", oldDir, kept, retained)
	}

	w = httptest.NewRecorder()
	s.handleCreateNamespace(w, newNamespaceRequest(t, http.MethodPost, "", CreateNamespaceRequest{Namespace: Namespace{"finance"}}))
	if w.Code != http.StatusOK {
		t.Fatalf("create namespace again status = %d (body: %s)", w.Code, w.Body.String())
	}

	newDir := createTableWithData(t, s, fc, "events")
	if newDir == oldDir {
		t.Fatalf("recreated table reuses the kept location %s", oldDir)
	}
	kept = filesUnder(fc, oldDir)
	slices.Sort(kept)
	if !slices.Equal(kept, retained) {
		t.Errorf("after creating the namespace and table again: files under %s = %v, want the retained %v", oldDir, kept, retained)
	}
}
