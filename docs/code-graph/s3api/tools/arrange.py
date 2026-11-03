"""Structure only: aggregate graphify symbol edges to file level, collapse
neighbor subpackages, count degrees. Never reads or writes curation."""
import json, sys, collections
from pathlib import Path
raw = json.load(open(sys.argv[1]))
layers = json.load(open(sys.argv[2])) if len(sys.argv) > 3 else {}
out = sys.argv[-1]
CORE = 'weed/s3api/'
FILE_PKGS = ('iceberg/', 'lance/', 's3vectors/')     # graphed file by file
COLLAPSED_EXTRA = ('weed/worker/tasks/iceberg/',)    # outside s3api, one node
def node_key(sf):
    if not sf: return None
    for pkg in COLLAPSED_EXTRA:
        if sf.startswith(pkg) and '/' not in sf[len(pkg):]: return pkg
    if not sf.startswith(CORE): return None
    rest = sf[len(CORE):]
    if '/' not in rest: return sf                      # core file
    pkg = rest.rsplit('/', 1)[0]                       # e.g. s3lifecycle/scheduler
    if pkg + '/' in FILE_PKGS: return sf               # satellite file node
    return CORE + pkg + '/'
nodes = {n['id']: n for n in raw['nodes']}
fmap = {nid: node_key(n.get('source_file')) for nid, n in nodes.items()}
agg = collections.defaultdict(lambda: {'w': 0, 'via': []})
dropped = collections.Counter()
for e in raw['edges']:
    s, t = fmap.get(e['source']), fmap.get(e['target'])
    if e['relation'] == 'contains' or e['relation'] == 'method':
        continue                                        # intra-file structure
    if s is None or t is None:
        dropped['out_of_scope' if s else 'sourceless_src'] += 1; continue
    if s == t: continue
    r = e['relation']
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
    core = not f.endswith('/')
    label = f[len(CORE):] if f.startswith(CORE) else f[len('weed/'):]
    out_nodes.append({'id': f, 'label': label, 'file': f, 'kind': 'file' if core else 'package',
                      'layer': layers.get(f), 'in': deg_in[f], 'out': deg_out[f], 'symbols': symcount[f]})
json.dump({'nodes': out_nodes, 'edges': edges}, open(out, 'w'), indent=1)
print('nodes', len(out_nodes), '(files', sum(n['kind']=='file' for n in out_nodes), 'pkgs', sum(n['kind']=='package' for n in out_nodes), ')',
      'edges', len(edges), 'pairs', len({(e['s'],e['t']) for e in edges}), file=sys.stderr)
print('by rel', collections.Counter(e['r'] for e in edges), file=sys.stderr)
print('dropped', dict(dropped), file=sys.stderr)
print('no-layer', [n['id'] for n in out_nodes if n['layer'] is None][:3], '...', sum(n['layer'] is None for n in out_nodes), file=sys.stderr)
zero_in = [n['id'] for n in out_nodes if n['in'] == 0]; print('in=0:', zero_in, file=sys.stderr)
