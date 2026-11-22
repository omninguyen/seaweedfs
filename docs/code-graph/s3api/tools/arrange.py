"""Structure only: check graphify symbol edges against go/types, aggregate them
to file level, collapse neighbor subpackages, count degrees. Never reads or
writes curation.
Usage: arrange.py <ast-raw.json> [layers.json] <out.json> --uses=<typecheck uses.json>"""
import json, sys, collections, os, re
from pathlib import Path
args = [a for a in sys.argv[1:] if not a.startswith('--uses=')]
uses_path = next(a[len('--uses='):] for a in sys.argv[1:] if a.startswith('--uses='))
raw = json.load(open(args[0]))
layers = json.load(open(args[1])) if len(args) > 2 else {}
out = args[-1]
CORE = 'weed/s3api/'
FILE_PKGS = ('s3tables/', 'iceberg/', 'lance/', 's3vectors/')     # graphed file by file
COLLAPSED_EXTRA = ('weed/filer/abstract_sql/',)  # outside s3api, one node
# Filer files graphed one node per file (lanes 12-19).
FILER_FILE_DIRS = ('weed/filer/', 'weed/filer/empty_folder_cleanup/', 'weed/pb/filer_pb/')
FILER_SERVER_PREFIX = 'weed/server/filer_grpc_server'
# Master, volume server, workers, admin scheduling and the client libraries
# (lanes 21-29): every file under these trees is a node, subpackages included.
CLUSTER_TREES = ('weed/wdclient/', 'weed/operation/', 'weed/topology/', 'weed/sequence/', 'weed/cluster/',
                 'weed/storage/', 'weed/worker/', 'weed/plugin/worker/', 'weed/admin/plugin/', 'weed/admin/topology/')
# Server files of the master (with its Raft and the HTTP helpers servers share)
# and of the volume server.
CLUSTER_SERVER_FILES = ('master_', 'raft_', 'volume_', 'common.go')
# Hand-written helpers next to generated code.
CLUSTER_PB_FILE_DIRS = ('weed/pb/master_pb/', 'weed/pb/volume_server_pb/')
# Code generated from each proto: one node per proto, named after its source.
GENERATED = {
    'weed/pb/filer_pb/filer.pb.go': 'weed/pb/filer.proto', 'weed/pb/filer_pb/filer_grpc.pb.go': 'weed/pb/filer.proto',
    'weed/pb/filer_pb/filer_vtproto.pb.go': 'weed/pb/filer.proto',
    'weed/pb/master_pb/master.pb.go': 'weed/pb/master.proto', 'weed/pb/master_pb/master_grpc.pb.go': 'weed/pb/master.proto',
    'weed/pb/volume_server_pb/volume_server.pb.go': 'weed/pb/volume_server.proto',
    'weed/pb/volume_server_pb/volume_server_grpc.pb.go': 'weed/pb/volume_server.proto',
    'weed/pb/plugin_pb/plugin.pb.go': 'weed/pb/plugin.proto', 'weed/pb/plugin_pb/plugin_grpc.pb.go': 'weed/pb/plugin.proto',
    'weed/pb/worker_pb/worker.pb.go': 'weed/pb/worker.proto', 'weed/pb/worker_pb/worker_grpc.pb.go': 'weed/pb/worker.proto',
}
GENERATED_NODES = set(GENERATED.values())
def node_key(sf):
    if not sf: return None
    if sf in GENERATED: return GENERATED[sf]
    if sf.startswith(CLUSTER_TREES): return sf
    if sf.startswith('weed/server/') and '/' not in sf[len('weed/server/'):] and sf[len('weed/server/'):].startswith(CLUSTER_SERVER_FILES):
        return sf
    for d in CLUSTER_PB_FILE_DIRS:
        if sf.startswith(d) and '/' not in sf[len(d):]: return sf
    for d in FILER_FILE_DIRS:
        if sf.startswith(d) and '/' not in sf[len(d):]: return sf
    if sf.startswith(FILER_SERVER_PREFIX) and '/' not in sf[len('weed/server/'):]: return sf
    for pkg in COLLAPSED_EXTRA:
        if sf.startswith(pkg) and '/' not in sf[len(pkg):]: return pkg
    if not sf.startswith(CORE): return None
    rest = sf[len(CORE):]
    if '/' not in rest: return sf                      # core file
    pkg = rest.rsplit('/', 1)[0]                       # e.g. s3lifecycle/scheduler
    if pkg + '/' in FILE_PKGS: return sf               # satellite file node
    return CORE + pkg + '/'
nodes = {n['id']: n for n in raw['nodes']}
# graphify moves a reference to a type declared in another file onto that
# declaration only when the type's name is unique in the corpus. Once several
# packages declare a name (Interval in filer and erasure_coding, Location in
# filer_pb, master_pb and wdclient), the reference stays on a sourceless stub
# and its edge is lost. In Go an unqualified type name is a type of the
# referencing file's own package (graphify keeps the qualifier, pkg.T, in a
# stub's label, and predeclared types and type parameters never become stubs),
# so a stub with a bare label is moved to that name's one declaration in the
# referencing file's directory, when there is exactly one.
BARE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
decl_in_dir = collections.defaultdict(list)
for nid, n in nodes.items():
    if n.get('source_file') and BARE.fullmatch(n['label']):
        decl_in_dir[(os.path.dirname(n['source_file']), n['label'])].append(nid)
dropped = collections.Counter()
for e in raw['edges']:
    t = nodes.get(e['target'])
    if t is None or t.get('source_file') or not BARE.fullmatch(t['label']): continue
    cands = decl_in_dir.get((os.path.dirname(e.get('source_file') or ''), t['label']), [])
    if len(cands) == 1:
        e['target'] = cands[0]; dropped['same_package_rewired'] += 1
fmap = {nid: node_key(n.get('source_file')) for nid, n in nodes.items()}
# graphify resolves a call it cannot type by the callee's name, and ids are
# case-insensitive, so it can draw an edge to the wrong declaration: a call of
# a closure or func-typed parameter (flush(), send(), write()) onto a method
# of that name in another package, BucketMetadata onto BucketMetaData,
# topology's Rack onto cluster.Rack. Every edge that crosses file nodes is
# checked against go/types (tools/_typecheck): the identifier it was drawn
# from, at its line (anywhere in its file when that line has none of that
# name), must denote a function, method or type declared in the target node.
# Otherwise the edge moves to the node of the one declaration the type checker
# names (v.DiskType() is the method in volume.go, not the type DiskType), or is
# dropped when that is a func-typed variable, lies outside the repository, or
# is not one declaration graphify has a node for.
LABEL_OF = {'type': '{}', 'func': '{}()', 'method': '.{}()', 'ifacemethod': '.{}()'}
typed_line = collections.defaultdict(list); typed_file = collections.defaultdict(list)
for f, line, name, kind, decl in json.load(open(uses_path))['uses']:
    d = None if kind == 'funcvar' or decl.startswith('EXT:') else (decl, LABEL_OF[kind].format(name))
    typed_line[(f, line, name)].append(d); typed_file[(f, name)].append(d)
by_file_label = collections.defaultdict(list)
for nid, n in nodes.items():
    if n.get('source_file'): by_file_label[(n['source_file'], n['label'])].append(nid)
untyped = set()
for i, e in enumerate(raw['edges']):
    if e['relation'] not in ('calls', 'indirect_call', 'references', 'embeds'): continue
    s, t = fmap.get(e['source']), fmap.get(e['target'])
    if s is None or t is None or s == t: continue
    f = e.get('source_file') or nodes[e['source']].get('source_file')
    line = int((e.get('source_location') or 'L0')[1:] or 0)
    label = nodes[e['target']]['label']
    name = label.lstrip('.').split('(')[0].split('[')[0]
    decls = typed_line.get((f, line, name)) or typed_file.get((f, name), [])
    if any(d and node_key(d[0]) == t for d in decls): continue
    found = {d for d in decls if d}
    moved = by_file_label.get(found.pop(), []) if len(found) == 1 else []
    if len(moved) == 1:
        e['target'] = moved[0]; dropped['typecheck_moved'] += 1
    else:
        untyped.add(i); dropped['typecheck_dropped'] += 1
agg = collections.defaultdict(lambda: {'w': 0, 'via': []})
for i, e in enumerate(raw['edges']):
    s, t = fmap.get(e['source']), fmap.get(e['target'])
    if e['relation'] == 'contains' or e['relation'] == 'method':
        continue                                        # intra-file structure
    if i in untyped: continue
    if s is None or t is None:
        dropped['out_of_scope' if s else 'sourceless_src'] += 1; continue
    if s == t: continue
    r = e['relation']
    # graphify resolves a Go package import to every file of that package. Into a
    # collapsed package node that is one edge; into a package graphed file by
    # file it is one edge per file, none of which says which file is used (the
    # calls/references edges do). Keep imports_from only into package nodes.
    if r == 'imports_from' and not t.endswith('/'):
        dropped['import_fanout_to_file'] += 1; continue
    a = agg[(s, t, r)]; a['w'] += 1
    if len(a['via']) < 4 and r != 'imports_from':
        a['via'].append(f"{nodes[e['source']]['label']} → {nodes[e['target']]['label']}")
files = sorted({k for k in fmap.values() if k})
edges = [{'s': s, 't': t, 'r': r, 'w': v['w'], 'via': v['via']} for (s, t, r), v in sorted(agg.items())]
deg_in = collections.Counter(); deg_out = collections.Counter()
for (s, t) in {(e['s'], e['t']) for e in edges}:
    deg_out[s] += 1; deg_in[t] += 1
symcount = collections.Counter(fmap[nid] for nid in nodes if fmap[nid])
out_nodes = []
for f in files:
    core = not f.endswith('/') and f not in GENERATED_NODES
    label = f[len(CORE):] if f.startswith(CORE) else f[len('weed/'):]
    if f in GENERATED_NODES: label = f[len('weed/'):] + ' (generated Go)'
    out_nodes.append({'id': f, 'label': label, 'file': f, 'kind': 'file' if core else 'package',
                      'layer': layers.get(f), 'in': deg_in[f], 'out': deg_out[f], 'symbols': symcount[f]})
json.dump({'nodes': out_nodes, 'edges': edges}, open(out, 'w'), indent=1)
print('nodes', len(out_nodes), '(files', sum(n['kind']=='file' for n in out_nodes), 'pkgs', sum(n['kind']=='package' for n in out_nodes), ')',
      'edges', len(edges), 'pairs', len({(e['s'],e['t']) for e in edges}), file=sys.stderr)
print('by rel', collections.Counter(e['r'] for e in edges), file=sys.stderr)
print('dropped', dict(dropped), file=sys.stderr)
print('no-layer', [n['id'] for n in out_nodes if n['layer'] is None][:3], '...', sum(n['layer'] is None for n in out_nodes), file=sys.stderr)
zero_in = [n['id'] for n in out_nodes if n['in'] == 0]; print('in=0:', zero_in, file=sys.stderr)
