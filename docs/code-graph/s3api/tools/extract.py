import sys, json
from pathlib import Path
import graphify
REPO = Path('/home/user/seaweedfs')
S3 = REPO / 'weed/s3api'
NEIGH = ['s3err','s3_constants','policy_engine','s3tables','s3lifecycle','lifecycle_xml','s3bucket','cors','s3lifecycle/scheduler','s3_objectlock','policy']
# Satellite packages graphed file by file (lanes 8-10), and the Iceberg
# maintenance worker, which arrange.py collapses to one node (lane 11).
FILE_PKGS = ['iceberg', 'lance', 's3vectors']
EXTRA_NEIGH = [REPO / 'weed/worker/tasks/iceberg']
# The filer the gateway stores everything in (lanes 12-19): the filer package's
# top-level files, its empty-folder cleaner, the hand-written filer_pb client
# code and the filer's gRPC server files are graphed file by file; abstract_sql
# and the code generated from filer.proto are collapsed by arrange.py.
FILER_FILE_DIRS = [REPO / 'weed/filer', REPO / 'weed/filer/empty_folder_cleanup', REPO / 'weed/pb/filer_pb']
FILER_COLLAPSED = [REPO / 'weed/filer/abstract_sql']
FILER_SERVER_GLOB = (REPO / 'weed/server', 'filer_grpc_server*.go')
EXCLUDE = {'weed/s3api/kms_azure.go', 'weed/s3api/policy_engine/examples.go',
           # test-support hooks other packages' tests call; not production paths
           'weed/filer/filer_notify_read_testing.go', 'weed/filer/meta_aggregator_testing.go'}
def ok(p):
    rel = str(p.relative_to(REPO))
    return p.suffix == '.go' and not p.name.endswith('_test.go') and rel not in EXCLUDE
seeds = sorted(p for p in S3.glob('*.go') if ok(p))
neigh = []
for d in NEIGH:
    neigh += sorted(p for p in (S3/d).glob('*.go') if ok(p))
for d in FILE_PKGS:
    seeds += sorted(p for p in (S3/d).glob('*.go') if ok(p))
for d in EXTRA_NEIGH + FILER_COLLAPSED:
    neigh += sorted(p for p in d.glob('*.go') if ok(p))
for d in FILER_FILE_DIRS:
    seeds += sorted(p for p in d.glob('*.go') if ok(p))
seeds += sorted(p for p in FILER_SERVER_GLOB[0].glob(FILER_SERVER_GLOB[1]) if ok(p))
print('seed files', len(seeds), 'neighbor files', len(neigh), file=sys.stderr)
res = graphify.extract(seeds + neigh, Path(sys.argv[1]), root=REPO)
json.dump(res, open(sys.argv[2], 'w'))
print({k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in res.items()}, file=sys.stderr)
