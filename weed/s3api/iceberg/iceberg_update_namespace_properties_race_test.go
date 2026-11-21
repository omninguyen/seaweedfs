package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// postNamespaceProperties sends an Iceberg REST UpdateNamespaceProperties
// request, the call Spark's ALTER NAMESPACE .. SET/UNSET PROPERTIES makes.
func (e *conflictEnv) postNamespaceProperties(body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/"+conflictBucket+"/namespaces/"+conflictNamespace+"/properties", strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	return rec
}

func (e *conflictEnv) namespaceProperties() map[string]string {
	e.t.Helper()
	var resp s3tables.GetNamespaceResponse
	e.adminDo("GetNamespace", &s3tables.GetNamespaceRequest{TableBucketARN: e.arn, Namespace: []string{conflictNamespace}}, &resp)
	return resp.Properties
}

func (e *conflictEnv) mustSetNamespaceProperties(body string) {
	e.t.Helper()
	if rec := e.postNamespaceProperties(body); rec.Code != http.StatusOK {
		e.t.Fatalf("seed %s: status %d: %s", body, rec.Code, rec.Body.String())
	}
}

// Two clients update different keys at the same time. The second lands while
// the first is between reading the properties and writing them back. Both are
// acknowledged, so both keys must be there afterwards.
func TestUpdateNamespacePropertiesKeepsConcurrentUpdate(t *testing.T) {
	e := newConflictEnv(t)
	e.mustSetNamespaceProperties(`{"updates":{"owner":"etl"}}`)

	var competing *httptest.ResponseRecorder
	e.filer.BeforeUpdate = func() {
		competing = e.postNamespaceProperties(`{"updates":{"b":"2"}}`)
	}
	rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`)

	if competing == nil {
		t.Fatal("the competing update never ran: the handler wrote nothing")
	}
	if competing.Code != http.StatusOK || rec.Code != http.StatusOK {
		t.Fatalf("statuses: competing %d %s, first %d %s", competing.Code, competing.Body.String(), rec.Code, rec.Body.String())
	}
	got := e.namespaceProperties()
	for key, want := range map[string]string{"owner": "etl", "a": "1", "b": "2"} {
		if got[key] != want {
			t.Errorf("property %s = %q, want %q (an acknowledged update was lost); properties: %v", key, got[key], want, got)
		}
	}
}

// A removal acknowledged while another client's update is in flight must stay
// removed: the update must not write the removed key back.
func TestUpdateNamespacePropertiesKeepsConcurrentRemoval(t *testing.T) {
	e := newConflictEnv(t)
	e.mustSetNamespaceProperties(`{"updates":{"owner":"etl","pii":"true"}}`)

	var competing *httptest.ResponseRecorder
	e.filer.BeforeUpdate = func() {
		competing = e.postNamespaceProperties(`{"removals":["pii"]}`)
	}
	rec := e.postNamespaceProperties(`{"updates":{"a":"1"}}`)

	if competing == nil {
		t.Fatal("the competing removal never ran: the handler wrote nothing")
	}
	if competing.Code != http.StatusOK || rec.Code != http.StatusOK {
		t.Fatalf("statuses: competing %d %s, first %d %s", competing.Code, competing.Body.String(), rec.Code, rec.Body.String())
	}
	var removed UpdateNamespacePropertiesResponse
	if err := json.Unmarshal(competing.Body.Bytes(), &removed); err != nil || len(removed.Removed) != 1 {
		t.Fatalf("competing removal summary = %s (%v), want pii removed", competing.Body.String(), err)
	}
	got := e.namespaceProperties()
	if v, ok := got["pii"]; ok {
		t.Errorf("property pii = %q came back after its acknowledged removal; properties: %v", v, got)
	}
	if got["a"] != "1" || got["owner"] != "etl" {
		t.Errorf("properties = %v, want owner=etl and a=1", got)
	}
}
