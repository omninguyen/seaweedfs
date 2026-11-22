// Command typecheck lists, for every identifier in the given packages, the
// function, method, func-typed variable or type it denotes and the file that
// declares it, as go/types resolves them. Structure only: arrange.py keeps a
// graphify edge only where the type checker agrees with its target, and moves
// it to the declaration the type checker names when they disagree.
//
// The leading underscore keeps this directory out of ./... in the SeaweedFS
// module. Usage, from the repository root:
//
//	go run docs/code-graph/s3api/tools/_typecheck/main.go <repo root> <out.json> <package patterns...>
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type listed struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	Export     string
	ImportMap  map[string]string
	DepOnly    bool
	Standard   bool
}

type mapImporter struct {
	m   map[string]string
	imp types.Importer
}

func (i mapImporter) Import(path string) (*types.Package, error) {
	if p, ok := i.m[path]; ok {
		path = p
	}
	return i.imp.Import(path)
}

func main() {
	repo, out, patterns := os.Args[1], os.Args[2], os.Args[3:]
	repo, _ = filepath.Abs(repo)
	// The module's language version: without it go/types rejects newer syntax
	// such as new(expr).
	goVersion := ""
	if mod, err := os.ReadFile(filepath.Join(repo, "go.mod")); err == nil {
		for _, line := range strings.Split(string(mod), "\n") {
			if f := strings.Fields(line); len(f) == 2 && f[0] == "go" {
				goVersion = "go" + f[1]
				break
			}
		}
	}
	cmd := exec.Command("go", append([]string{"list", "-e", "-export", "-deps", "-json"}, patterns...)...)
	cmd.Dir = repo
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	dec := json.NewDecoder(stdout)
	exports := map[string]string{}
	var targets []*listed
	for {
		p := &listed{}
		if err := dec.Decode(p); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
		if !p.DepOnly && !p.Standard {
			targets = append(targets, p)
		}
	}
	if err := cmd.Wait(); err != nil {
		fmt.Fprintln(os.Stderr, "go list:", err)
	}
	fset := token.NewFileSet()
	gc := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		f, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(f)
	})
	rel := func(obj types.Object) string {
		fn := fset.Position(obj.Pos()).Filename
		if strings.HasPrefix(fn, repo+"/") {
			return strings.TrimPrefix(fn, repo+"/")
		}
		if obj.Pkg() != nil {
			return "EXT:" + obj.Pkg().Path()
		}
		return "EXT:"
	}
	seen := map[string]bool{}
	var uses [][]interface{}
	typeErrs := map[string]int{}
	for _, p := range targets {
		var files []*ast.File
		for _, name := range append(append([]string{}, p.GoFiles...), p.CgoFiles...) {
			f, err := parser.ParseFile(fset, filepath.Join(p.Dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				typeErrs[p.ImportPath]++
				continue
			}
			files = append(files, f)
		}
		info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
		conf := types.Config{Importer: mapImporter{p.ImportMap, gc}, FakeImportC: true, GoVersion: goVersion,
			Error: func(err error) {
				if typeErrs[p.ImportPath] < 3 {
					fmt.Fprintln(os.Stderr, "type error:", err)
				}
				typeErrs[p.ImportPath]++
			}}
		conf.Check(p.ImportPath, fset, files, info)
		for id, obj := range info.Uses {
			var kind string
			switch o := obj.(type) {
			case *types.Func:
				kind = "func"
				if sig, ok := o.Type().(*types.Signature); ok && sig.Recv() != nil {
					kind = "method"
					if types.IsInterface(sig.Recv().Type()) {
						kind = "ifacemethod"
					}
				}
			case *types.Var:
				if _, ok := o.Type().Underlying().(*types.Signature); !ok {
					continue
				}
				kind = "funcvar"
			case *types.TypeName:
				kind = "type"
			default:
				continue
			}
			pos := fset.Position(id.Pos())
			file := strings.TrimPrefix(pos.Filename, repo+"/")
			row := []interface{}{file, pos.Line, id.Name, kind, rel(obj)}
			key := fmt.Sprint(row...)
			if !seen[key] {
				seen[key] = true
				uses = append(uses, row)
			}
		}
	}
	sort.Slice(uses, func(i, j int) bool { return fmt.Sprint(uses[i]...) < fmt.Sprint(uses[j]...) })
	b, _ := json.Marshal(map[string]interface{}{"uses": uses, "type_errors": typeErrs, "packages": len(targets)})
	if err := os.WriteFile(out, b, 0o644); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "packages %d, uses %d, packages with type errors %d\n", len(targets), len(uses), len(typeErrs))
}
