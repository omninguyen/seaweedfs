import sys, json
from pathlib import Path
import graphify
REPO = Path('/home/user/seaweedfs')
S3 = REPO / 'weed/s3api'
NEIGH = ['s3err','s3_constants','policy_engine','s3lifecycle','lifecycle_xml','s3bucket','cors','s3lifecycle/scheduler','s3_objectlock','policy']
# Satellite packages graphed file by file (the S3 Tables catalog, lane 20, and
# lanes 8-10). s3tables/s3tablestest is test support and stays out: only the
# package's top-level files are globbed.
FILE_PKGS = ['s3tables', 'iceberg', 'lance', 's3vectors']
# The filer the gateway stores everything in (lanes 12-19): the filer package's
# top-level files, its empty-folder cleaner, the hand-written filer_pb client
# code and the filer's gRPC server files are graphed file by file; abstract_sql
# and the code generated from filer.proto are collapsed by arrange.py.
FILER_FILE_DIRS = [REPO / 'weed/filer', REPO / 'weed/filer/empty_folder_cleanup', REPO / 'weed/pb/filer_pb']
FILER_COLLAPSED = [REPO / 'weed/filer/abstract_sql']
FILER_SERVER_GLOB = (REPO / 'weed/server', 'filer_grpc_server*.go')
# The master, the volume servers, the workers and the admin side that schedules
# them (lanes 21-29), with the client libraries the gateway and filer reach the
# master and volume servers through: every file of these trees, subpackages
# included, is one node (the Iceberg maintenance worker, once one collapsed
# node, now too). Scope is Linux with default build tags; see EXCLUDE.
CLUSTER_FILE_TREES = [REPO / d for d in (
    'weed/wdclient', 'weed/operation', 'weed/topology', 'weed/sequence', 'weed/cluster',
    'weed/storage', 'weed/worker', 'weed/plugin/worker', 'weed/admin/plugin', 'weed/admin/topology')]
CLUSTER_SERVER_GLOBS = [(REPO / 'weed/server', g) for g in ('master_*.go', 'raft_*.go', 'common.go', 'volume_*.go')]
# Code generated from these protos, and their hand-written helpers, is extracted
# so calls into it resolve; arrange.py folds each proto's generated files into
# one node, as it does for filer.proto.
CLUSTER_PB_DIRS = [REPO / 'weed/pb' / p for p in ('master_pb', 'volume_server_pb', 'plugin_pb', 'worker_pb')]
EXCLUDE = {'weed/s3api/kms_azure.go', 'weed/s3api/policy_engine/examples.go',
           # test-support hooks other packages' tests call; not production paths
           'weed/filer/filer_notify_read_testing.go', 'weed/filer/meta_aggregator_testing.go',
           'weed/worker/tasks/iceberg/testing_api.go',
           # Linux, default build tags: files built only on Windows or on systems
           # other than Linux, and optional build-tag variants (5BytesOffset, rclone)
           'weed/storage/io_error_windows.go', 'weed/storage/volume_allocated_windows.go',
           'weed/storage/store_vacuum_fs_windows.go', 'weed/storage/backend/memory_map/memory_map_windows.go',
           'weed/storage/backend/memory_map/os_overloads/file_windows.go',
           'weed/storage/backend/memory_map/os_overloads/types_windows.go',
           'weed/storage/backend/memory_map/os_overloads/syscall_windows.go',
           'weed/storage/backend/volume_create_windows.go', 'weed/storage/backend/volume_open.go',
           'weed/storage/backend/volume_create.go', 'weed/storage/types/offset_5bytes.go',
           'weed/storage/backend/rclone_backend/dummy.go', 'weed/storage/backend/rclone_backend/rclone_backend.go',
           'weed/storage/backend/rclone_backend/rclone_progress.go'}
def ok(p):
    rel = str(p.relative_to(REPO))
    return p.suffix == '.go' and not p.name.endswith('_test.go') and rel not in EXCLUDE
seeds = sorted(p for p in S3.glob('*.go') if ok(p))
neigh = []
for d in NEIGH:
    neigh += sorted(p for p in (S3/d).glob('*.go') if ok(p))
for d in FILE_PKGS:
    seeds += sorted(p for p in (S3/d).glob('*.go') if ok(p))
for d in FILER_COLLAPSED:
    neigh += sorted(p for p in d.glob('*.go') if ok(p))
for d in FILER_FILE_DIRS:
    seeds += sorted(p for p in d.glob('*.go') if ok(p))
seeds += sorted(p for p in FILER_SERVER_GLOB[0].glob(FILER_SERVER_GLOB[1]) if ok(p))
for d in CLUSTER_FILE_TREES:
    seeds += sorted(p for p in d.rglob('*.go') if ok(p))
for d, g in CLUSTER_SERVER_GLOBS:
    seeds += sorted(p for p in d.glob(g) if ok(p))
for d in CLUSTER_PB_DIRS:
    seeds += sorted(p for p in d.glob('*.go') if ok(p))
print('seed files', len(seeds), 'neighbor files', len(neigh), file=sys.stderr)
res = graphify.extract(seeds + neigh, Path(sys.argv[1]), root=REPO)
json.dump(res, open(sys.argv[2], 'w'))
print({k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in res.items()}, file=sys.stderr)
