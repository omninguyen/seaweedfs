package iceberg

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const dropTestBucket = "warehouse"

func newTableRequest(t *testing.T, method, namespace, table, query string) *http.Request {
	t.Helper()
	target := "/v1/" + dropTestBucket + "/namespaces/" + namespace + "/tables/" + table
	if query != "" {
		target += "?" + query
	}
	r := httptest.NewRequest(method, target, nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": dropTestBucket, "namespace": namespace, "table": table})
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
}

// createTableWithData creates finance.<name> through the handler and adds a
// data file under its location, returning the table's location in the filer.
func createTableWithData(t *testing.T, s *Server, fc *memFiler, name string) string {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleCreateTable(w, newCreateTableRequest(t, dropTestBucket, "finance", createTableBody(name, oneBasedSchema, "", ""), "alice"))
	if w.Code != http.StatusOK {
		t.Fatalf("create %s: status = %d (body: %s)", name, w.Code, w.Body.String())
	}
	var result struct {
		Metadata struct {
			Location string `json:"location"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	_, tablePath, err := parseS3Location(result.Metadata.Location)
	if err != nil {
		t.Fatalf("parse location %q: %v", result.Metadata.Location, err)
	}
	dir := path.Join(s3tables.TablesPath, dropTestBucket, tablePath)
	fc.seed(path.Join(dir, "data"), &filer_pb.Entry{Name: "data", IsDirectory: true})
	fc.seed(path.Join(dir, "data", "00000-0-data.parquet"), &filer_pb.Entry{Name: "00000-0-data.parquet"})
	return dir
}

func filesUnder(fc *memFiler, dir string) []string {
	var files []string
	for p, entry := range fc.entries {
		if strings.HasPrefix(p, dir+"/") && !entry.IsDirectory {
			files = append(files, strings.TrimPrefix(p, dir+"/"))
		}
	}
	return files
}

func loadStatus(t *testing.T, s *Server, name string) int {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleLoadTable(w, newTableRequest(t, http.MethodGet, "finance", name, ""))
	return w.Code
}

// Only purgeRequested=true deletes the table's files. The spec defaults the
// parameter to false, and Spark sends DROP TABLE without it.
func TestDropTableHonorsPurgeRequested(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		wantFiles bool
	}{
		{"no purgeRequested keeps the files", "", true},
		{"purgeRequested=false keeps the files", "purgeRequested=false", true},
		{"purgeRequested=true deletes the files", "purgeRequested=true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, dropTestBucket, "finance", "alice")
			s := NewServer(fc, nil)
			dir := createTableWithData(t, s, fc, "events")
			if files := filesUnder(fc, dir); len(files) < 2 {
				t.Fatalf("setup: want a metadata and a data file under %s, have %v", dir, files)
			}

			w := httptest.NewRecorder()
			s.handleDropTable(w, newTableRequest(t, http.MethodDelete, "finance", "events", tc.query))
			if w.Code != http.StatusNoContent {
				t.Fatalf("drop status = %d, want %d (body: %s)", w.Code, http.StatusNoContent, w.Body.String())
			}
			if got := loadStatus(t, s, "events"); got != http.StatusNotFound {
				t.Errorf("load after drop = %d, want %d", got, http.StatusNotFound)
			}

			files := filesUnder(fc, dir)
			if tc.wantFiles {
				for _, want := range []string{"metadata/v1.metadata.json", "data/00000-0-data.parquet"} {
					if !slices.Contains(files, want) {
						t.Errorf("%s was deleted; files left: %v", want, files)
					}
				}
			} else if len(files) != 0 {
				t.Errorf("purge left files behind: %v", files)
			}
		})
	}
}

// A malformed purgeRequested is the client's error, and the table is not
// dropped on a guess.
func TestDropTableRejectsInvalidPurgeRequested(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, dropTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	dir := createTableWithData(t, s, fc, "events")

	w := httptest.NewRecorder()
	s.handleDropTable(w, newTableRequest(t, http.MethodDelete, "finance", "events", "purgeRequested=maybe"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("drop status = %d, want %d (body: %s)", w.Code, http.StatusBadRequest, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "BadRequestException") {
		t.Errorf("body = %s, want a BadRequestException", w.Body.String())
	}
	if got := loadStatus(t, s, "events"); got != http.StatusOK {
		t.Errorf("load after rejected drop = %d, want %d", got, http.StatusOK)
	}
	if files := filesUnder(fc, dir); len(files) < 2 {
		t.Errorf("rejected drop touched the files: %v", files)
	}
}

// After a plain drop the kept files stay where they are: a table created
// again under the same name goes to a fresh location instead of writing over
// them.
func TestCreateTableAfterNonPurgingDropKeepsOldFiles(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, dropTestBucket, "finance", "alice")
	s := NewServer(fc, nil)
	oldDir := createTableWithData(t, s, fc, "events")
	oldFiles := filesUnder(fc, oldDir)

	w := httptest.NewRecorder()
	s.handleDropTable(w, newTableRequest(t, http.MethodDelete, "finance", "events", ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("drop status = %d (body: %s)", w.Code, w.Body.String())
	}

	newDir := createTableWithData(t, s, fc, "events")
	if newDir == oldDir {
		t.Fatalf("recreated table reuses the kept location %s", oldDir)
	}
	if got := loadStatus(t, s, "events"); got != http.StatusOK {
		t.Errorf("load recreated table = %d, want %d", got, http.StatusOK)
	}
	for _, f := range oldFiles {
		if !slices.Contains(filesUnder(fc, oldDir), f) {
			t.Errorf("recreating the table removed the kept file %s", f)
		}
	}
}
