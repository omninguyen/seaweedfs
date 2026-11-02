"""Assemble the viewer by plain marker replace, then run the Step-7a data checks.
Usage: python3 tools/assemble.py <graph_dir> <repo_root>"""
import json, sys, subprocess
from pathlib import Path
G, REPO = Path(sys.argv[1]), Path(sys.argv[2])
meta = json.loads((G/'meta.json').read_text())
data = json.loads((G/'data.json').read_text())
cur = json.loads((G/'curation.json').read_text()) if (G/'curation.json').exists() else {}
def js(o):  # safe inside <script type=application/json>
    return json.dumps(o, ensure_ascii=False).replace('</', '<\\/')
html = (G/'viewer-template.html').read_text()
for marker, val in [('__TITLE__', meta['title']), ('__SUBTITLE__', meta['subtitle']), ('__INTRO__', meta['intro']),
                    ('__LAYERS__', js(meta['layers'])), ('__DATA__', js(data)), ('__CURATION__', js(cur))]:
    assert marker in html, marker
    html = html.replace(marker, val)
(G/meta['output']).write_text(html)
ids = {n['file'] for n in data['nodes']}
orphans = sorted(k for k in cur if k not in ids)
phantoms = sorted(n['file'] for n in data['nodes'] if not (REPO/n['file']).exists())
uncurated = [n['file'] for n in data['nodes'] if not cur.get(n['file'], {}).get('what')]
declared = {l['id'] for l in meta['layers']}; used = {n['layer'] for n in data['nodes']}
print(f"wrote {meta['output']}: {len(data['nodes'])} nodes, {len(data['edges'])} edges")
print(f"orphan curation keys: {len(orphans)} {orphans[:5]}")
print(f"phantom nodes (file missing on disk): {len(phantoms)} {phantoms[:5]}")
print(f"uncurated nodes: {len(uncurated)}")
print(f"lanes declared {sorted(declared)}; lanes with nodes {sorted(used)}; undeclared used: {sorted(used-declared)}")
