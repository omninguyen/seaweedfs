"""Additive-only merge of a curation batch into curation.json.

Adds a field only when the target key does not already hold it. An existing,
different value is NEVER overwritten: it is reported as a conflict and left
alone (hand-edit curation.json to change curated text on purpose)."""
import json, sys
from pathlib import Path
cur_path, batch_path = Path(sys.argv[1]), Path(sys.argv[2])
cur = json.loads(cur_path.read_text()) if cur_path.exists() else {}
batch = json.loads(batch_path.read_text())
added = conflicts = 0
for key, fields in batch.items():
    slot = cur.setdefault(key, {})
    for f, v in fields.items():
        if f not in slot or slot[f] in ('', None, []):
            slot[f] = v; added += 1
        elif slot[f] != v:
            conflicts += 1; print(f'CONFLICT kept existing: {key}.{f}', file=sys.stderr)
tmp = cur_path.with_suffix('.tmp')
tmp.write_text(json.dumps(cur, indent=1, ensure_ascii=False, sort_keys=True) + '\n')
tmp.replace(cur_path)
print(f'added {added} fields, {conflicts} conflicts, {len(cur)} curated keys total')
