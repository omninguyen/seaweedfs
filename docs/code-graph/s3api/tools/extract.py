import sys, json
from pathlib import Path
import graphify
REPO = Path('/home/user/seaweedfs')
S3 = REPO / 'weed/s3api'
NEIGH = ['s3err','s3_constants','policy_engine','s3tables','s3lifecycle','lifecycle_xml','s3bucket','cors','s3lifecycle/scheduler','s3_objectlock','policy']
EXCLUDE = {'weed/s3api/kms_azure.go', 'weed/s3api/policy_engine/examples.go'}
def ok(p):
    rel = str(p.relative_to(REPO))
    return p.suffix == '.go' and not p.name.endswith('_test.go') and rel not in EXCLUDE
seeds = sorted(p for p in S3.glob('*.go') if ok(p))
neigh = []
for d in NEIGH:
    neigh += sorted(p for p in (S3/d).glob('*.go') if ok(p))
print('seed files', len(seeds), 'neighbor files', len(neigh), file=sys.stderr)
res = graphify.extract(seeds + neigh, Path(sys.argv[1]), root=REPO)
json.dump(res, open(sys.argv[2], 'w'))
print({k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in res.items()}, file=sys.stderr)
