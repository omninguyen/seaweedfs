package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// The summary lists every updated key and passes through what the write found
// for the removals.
func TestNamespacePropertiesSummary(t *testing.T) {
	summary := namespacePropertiesSummary(nil, []string{"abc"}, map[string]string{"prop": "no"})
	if !reflect.DeepEqual(summary.Removed, []string{}) {
		t.Errorf("Removed = %v, want []", summary.Removed)
	}
	if !reflect.DeepEqual(summary.Updated, []string{"prop"}) {
		t.Errorf("Updated = %v, want [prop]", summary.Updated)
	}
	if !reflect.DeepEqual(summary.Missing, []string{"abc"}) {
		t.Errorf("Missing = %v, want [abc]", summary.Missing)
	}
}

// A key in both removals and updates is rejected before any backend call, so
// the conflict path is reachable without a filer.
func TestHandleUpdateNamespacePropertiesConflict(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ns/properties",
		strings.NewReader(`{"removals":["x"],"updates":{"x":"1"}}`))
	req = mux.SetURLVars(req, map[string]string{"namespace": "ns"})
	rec := httptest.NewRecorder()

	(&Server{}).handleUpdateNamespaceProperties(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Type != "UnprocessableEntityException" {
		t.Fatalf("error type = %q, want UnprocessableEntityException", resp.Error.Type)
	}
}
