# weed/s3api and filer code graph

A file-level map of the SeaweedFS S3 gateway, the catalog APIs that run beside it, and the filer it runs on. The structure comes from the AST and the explanations were written by hand. Open `s3api-graph.html` in a browser. It is self-contained, with no server or network needed.

## What is in it

- **238 nodes:**
  - the 101 non-test files of package `weed/s3api`;
  - the 40 non-test files of the three catalog servers wired next to the gateway in `command/s3.go`: the Iceberg REST catalog (`weed/s3api/iceberg`, 21 files), the Lance Namespace REST server (`weed/s3api/lance`, 9) and the S3 Vectors API (`weed/s3api/s3vectors`, 10);
  - the filer the gateway runs on: the 58 non-test files of `weed/filer`, the 2 of `weed/filer/empty_folder_cleanup`, the 7 hand-written files of `weed/pb/filer_pb`, one node for the generated gRPC code (`weed/pb/filer.proto`, standing for `filer.pb.go`, `filer_grpc.pb.go` and `filer_vtproto.pb.go`) and the 16 `weed/server/filer_grpc_server*.go` files;
  - 13 collapsed packages, one node each: the 11 neighbor packages package `s3api` imports (`s3err`, `s3_constants`, `policy_engine`, `s3tables`, `s3lifecycle` (top-level files only), `s3lifecycle/scheduler`, `lifecycle_xml`, `s3bucket`, `cors`, `s3_objectlock`, `policy`), the Iceberg maintenance worker `weed/worker/tasks/iceberg` and the shared SQL store `weed/filer/abstract_sql`.
- **1,531 edges** (1,187 distinct node pairs), extracted with graphify and aggregated from symbols to files. Edge types are `calls`, `indirect_call` (function values such as route registration), `references`, `embeds` and `imports_from`. No edge was added by hand. graphify resolves a Go package import to every file of that package; those fan-out edges are dropped when the target is a file node (`arrange.py`, `import_fanout_to_file`), so `imports_from` edges only point at collapsed package nodes.
- **20 lanes, ordered from the S3 request down to the filer:**

| Lane | Contents |
| --- | --- |
| 0 Cross-cutting | Constants, error codes, XML types, metrics |
| 1 Server & routing | Server construction, routing, circuit breaker, path validation |
| 2 Auth & identity | SigV4/V2, IAM, STS, bucket policies, ACLs |
| 3 Bucket API | Bucket handlers and per-bucket config |
| 4 Object API | Object handlers |
| 5 Object features | Versioning, SSE, Object Lock, multipart, lifecycle TTL |
| 6 Filer access | How the gateway reads and writes filer entries |
| 7 Satellite APIs | S3 Tables, lifecycle RPC, SOSAPI |
| 8 Iceberg REST catalog | `weed/s3api/iceberg` |
| 9 Lance REST namespace | `weed/s3api/lance` |
| 10 S3 Vectors | `weed/s3api/s3vectors` |
| 11 Iceberg maintenance worker | `weed/worker/tasks/iceberg` (collapsed) |
| 12 Filer protocol | `filer.proto` and the `filer_pb` client helpers |
| 13 Filer gRPC server | `weed/server/filer_grpc_server*.go` |
| 14 Filer namespace operations | `filer.go`, deletes, renames, listing, hard links, buckets |
| 15 Entries, stores and filer configuration | Entry model and codec, store interface and wrapper, `filer.conf`, `abstract_sql/` |
| 16 Chunks and the read path | Chunk lists, manifests, visible intervals, reader caches, streaming |
| 17 Metadata events and sync | Metadata log, subscriptions, peer aggregation and replay |
| 18 Remote storage | Remote mounts, lazy fetch and listing, eviction, tombstones |
| 19 Filer housekeeping | Chunk deletion and its ledger, the empty-folder cleaner |

- **Excluded:** `*_test.go`, test helper packages (`s3tablestest`, `lifecycletest`), `testdata` directories, `policy_engine/examples.go`, `kms_azure.go`, and the test-only hooks `weed/filer/filer_notify_read_testing.go` and `weed/filer/meta_aggregator_testing.go`. The individual filer store backends (`weed/filer/leveldb*`, `mysql`, `redis*` and so on) are outside the scope; only the shared SQL implementation is a node.
- **A known gap in the edges:** the Iceberg, Lance and S3 Vectors servers register their routes with unexported method values (`s.handleX`), and graphify draws no edge for those. Their handler files therefore show no incoming edge from `server.go`. The gateway's own routes (`s3a.XHandler`) are drawn. The gateway calls the filer over gRPC, so the only AST edge from lanes 0–11 into the filer gRPC server (lane 13) is a name collision (`s3api_object_handlers_put.go` → `filer_grpc_server.go` via a method named `lookupFileId`); the generated client in `weed/pb/filer.proto` is where the two sides meet. The 124 gateway-to-filer edges go to that node, the `filer_pb` helpers and the filer library code the gateway links in directly (chunk lists, manifests, streaming, `filer.conf`, config file reads).

Each node was curated by reading its source in full. The collapsed worker node was curated from its handler, detection, commit, orphan and filer I/O code; its algorithm files (compaction, delete rewrite, row lineage, geometry) are summarized, not described one by one. The generated-code node was curated from `filer.proto`. The panel shows these fields:

| Field | Content |
| --- | --- |
| character | One-line role |
| what | What the code does |
| why | The consequence if it fails, the design decision behind it, and any compatibility constraint (AWS, Iceberg REST, Lance Namespace, POSIX) |
| when | When it runs |
| ux | What users see |
| if_broken | What breaks if it does |
| gotchas | Non-obvious behavior to know before changing it |
| issue / issue_note | The defect flag and its explanation, where one applies |

## Flagged defects (30)

"Probed" means the defect was reproduced with a throwaway test that was then deleted. "Read" means it was found by reading the code and is not reproduced. The 20 bug rows carried over from the earlier rounds were checked again at master `b9866bae` and re-read at `917cf534`. The `s3api_object_handlers_list.go` row was probed on SeaweedFS 4.53 and master `d51a7933` (boto3 and Spark); the two filer rows were found while curating at `917cf534`.

| Kind | Node | Summary | Check |
| --- | --- | --- | --- |
| bug | `s3err/` | `ErrMalformedCredentialDate` has no response entry, so `WriteErrorResponse` panics (`WriteHeader(0)`); reachable without authentication. | probed |
| bug | `chunked_reader_v4.go` | STREAMING-…-TRAILER uploads: chunk and trailer signatures are not enforced, so the body is unauthenticated. | probed |
| bug | `policy_engine/` | `s3:authType` is not set for SigV4 presigned URLs, so "deny presigned" policies do not apply to them. | probed |
| bug | `s3api_embedded_iam.go` | `CreatePolicyVersion` rejects Deny / non-s3 documents that `CreatePolicy` accepts, so such policies can never be updated (Terraform). | probed |
| bug | `s3tables/` | CreateTable and CreateView make the record directory non-exclusively, so a create that lands after another creator has finished wipes that creator's record: both answer 200 and the first table is gone from the catalog. | probed |
| bug | `cors/` | A preflight uses the first rule whose origin matches even when its methods don't, so a later rule that allows PUT is never used. | probed |
| bug | `s3bucket/` | `VerifyS3BucketName` accepts non-ASCII letters and digits ("bücket"), and CreateBucket relies on it alone. | probed |
| bug | `s3api_put_handlers.go` | SSE-S3 multipart parts reuse the AES-CTR keystream (shared DEK and base IV, part offset 0). | probed |
| bug | `iceberg/handlers_table.go` | DropTable ignores `purgeRequested` and always deletes the table's data; S3 Tables `DeleteTable` deletes it as well. | probed |
| bug | `iceberg/handlers_namespace.go` | UpdateNamespaceProperties is a read-modify-write without a version check, so concurrent property updates drop each other. | probed |
| bug | `s3api_public_access_block.go` | `BlockPublicPolicy` and `RestrictPublicBuckets` are accepted and returned but never enforced, so a public bucket policy is still accepted on a bucket that claims to block it. | probed |
| bug | `worker/tasks/iceberg/` | The maintenance worker writes records that a rename or delete has claimed, leaving a table stuck in `Deleting=rename` (409 on every later commit and delete). | probed |
| bug | `auth_signature_v2.go` | Header-signed SigV2 has no Date freshness check, so a captured request can be replayed until the key is rotated. (The missing-subresource half was fixed by #12575.) | probed |
| bug | `s3api_object_handlers_multipart.go` | `isVersioningEnabled` is passed as `objectLockEnabled`: lock headers are accepted on non-lock buckets and are not enforced. | probed |
| bug | `s3api_object_handlers_copy.go` | Copied chunks leak when finalize fails (for example a 412); UploadPartCopy misses `prefixObjectSource`. | probed |
| bug | `s3api_bucket_policy_handlers.go` | Unconditional whole-entry bucket rewrite: a concurrent versioning or Object Lock config write can be reverted. | probed |
| bug | `s3api_server.go` | `ValidateS3Credential` compares secrets with `!=` (a timing side channel on the OAuth token endpoints); low severity. | read |
| bug | `iceberg/handlers_commit.go` | A non-conflict UpdateTable failure deletes the staged metadata file although the record may point at it. | probed |
| bug | `iceberg/handlers_transaction.go` | The rollback of a failed multi-table commit carries no version token and reverts commits other writers landed meanwhile. | probed |
| bug | `iceberg/handlers_view.go` | CreateView answers 200 with the stored definition for a view that already exists (the spec says 409), and with the same client-supplied location the loser of a concurrent create overwrites the winner's metadata file. | probed |
| bug | `s3api_object_handlers_list.go` | On an unversioned bucket a delimited listing returns an empty filer directory as a CommonPrefix for about two minutes after its last object is deleted (until the empty-folder cleaner runs); Spark's FileOutputCommitter v1 append with partitionBy then fails. | probed |
| bug | `filer/stream.go` | `ChunkStreamReader` stops at the first hole of a sparse file and reports success, so the S3 replication sink (filer.backup / filer.sync) uploads such files truncated. | probed |
| bug | `server/filer_grpc_server_rename.go` | `StreamRenameEntry` returns on a non-empty target directory without rolling back its store transaction; on SQL stores each such rename holds a pooled connection until restart. | read |
| dead | `s3_sse_kms.go` | The S3 Bucket Key data-key cache path is not wired in. | read |
| dead | `s3api_copy_size_calculation.go` | Its only output is a V(2) log line. | read |
| dead | `filer_util_tags.go` | `getTags` has no callers. | read |
| duplicate | `s3api_object_handlers.go` | GET and HEAD each carry a ~90-line copy of versioned entry resolution. | read |
| duplicate | `filer_multipart.go` | Three near-identical decorate closures in `completeMultipartUpload`. | read |
| duplicate | `s3lifecycle/` | `version_time.go` copies the gateway's version-ID comparator. | read |
| duplicate | `lance/handlers_oauth.go` | A near line-for-line copy of the Iceberg catalog's OAuth token code. | read |

Use the **Defects** chips in the viewer to show only flagged nodes.

### Fixed since the first curation

- `s3api_object_handlers_put.go`: an invalid `x-amz-storage-class` was rejected only after the upload and leaked the chunks. Fixed upstream in [seaweedfs/seaweedfs#12354](https://github.com/seaweedfs/seaweedfs/pull/12354) (`validatePutHeaders` now runs before the body is read); the flag was removed.
- `iceberg/commit_helpers.go`: create-on-commit wrote v{N}.metadata.json non-exclusively and deleted it on any CreateTable error, so concurrent creators could delete the winner's file. Fixed upstream in [seaweedfs/seaweedfs#12474](https://github.com/seaweedfs/seaweedfs/pull/12474) (`195f815`): both create paths stage the file exclusively with a per-request token, discard it only when the catalog refused the registration, and answer 409 on an existing name. The flag was removed, and the CreateTable part of the `iceberg/handlers_table.go` flag with it. The fix's tests pass at `7506e5f`.
- `s3tables/`: three defects of the first flag are fixed upstream, and each was re-checked at `b9866bae`. A table renamed across namespaces can commit again ([seaweedfs/seaweedfs#12515](https://github.com/seaweedfs/seaweedfs/pull/12515), `2143761`). `RenameTable` refuses a source that changed between its reads, and has a REST route ([seaweedfs/seaweedfs#12446](https://github.com/seaweedfs/seaweedfs/pull/12446), `f71c0fe`). Policy `Condition` blocks are validated when stored and an unusable stored one fails safe ([seaweedfs/seaweedfs#12389](https://github.com/seaweedfs/seaweedfs/pull/12389), `15ff5c1`). The node keeps a bug flag for a different defect, found while re-checking.
- `iceberg/handlers_view.go`: since [seaweedfs/seaweedfs#12475](https://github.com/seaweedfs/seaweedfs/pull/12475) a view created without a location has its own directory, so the loser of a concurrent create no longer overwrites the winner's metadata file in that case. The flag stays for the same-location case and for the 200 answer on an existing view.

- `lance/handlers_table.go`: deregister and re-register needed only read permission. Fixed upstream in [seaweedfs/seaweedfs#12552](https://github.com/seaweedfs/seaweedfs/pull/12552) (`eb159f3`): both now require UpdateTable. The flag was removed.
- `auth_signature_v2.go`: the subresources missing from the SigV2 string to sign (`?cors`, `?tagging`, `?object-lock`, `?encryption` and others) are fixed by [seaweedfs/seaweedfs#12575](https://github.com/seaweedfs/seaweedfs/pull/12575) (`917cf53`). The flag stays for the missing Date freshness check.
- Removed from the gotchas during the `917cf534` sync because upstream fixed them: the null-version delete that treated any lookup error as "already gone" (#12482, `s3api_object_versioning.go`), the upload-throttle overshoot (#12559, `s3api_circuit_breaker.go`) and the policy-condition panic on JSON null (#12389, `policy_engine/`).

## Provenance

- Source: branch `claude/brave-carson-13bjg1`, with `weed/` equal to master `917cf534`. This folder is the only addition.
- History of the curation: the first 109 nodes were curated at `984eac6` and drift-checked to `199b2e2`; the 36 catalog-server and worker nodes were added at `199b2e2`; a second sync went to `995084f`; three Iceberg nodes were re-read at `7506e5f` for #12474; every bug flag was re-checked at `b9866bae`.
- Third sync, `995084f` → `917cf534`: the AST was extracted again with the filer seeds added, giving 238 nodes. The 54 existing nodes whose files changed were re-read against their diffs and updated (lane by lane, with the upstream PR behind each change named in the text); the 7 new `weed/s3api` and `s3vectors` files and all 85 filer, `filer_pb` and filer gRPC server nodes were curated by reading each file in full at `917cf534`. Every node now describes `917cf534`, and the edges are extracted from it.
- Edges that disappeared in the sync were checked against the code. Four follow refactors: `s3_iam_middleware` → `s3_action_resolver` and `auth_credentials` → `s3api_bucket_handlers` (the action and resource ARN now come from `resolveS3AuthTarget`), `postpolicy` → `s3api_acl_helper` and `postpolicy` → `public_access_block` (the POST Object ACL moved into `preparePostObjectACL`). The fifth, `s3api_object_handlers_put` → `filer_multipart` through `add()`, was a name collision between a local closure and `multipartUploadPage.add`.
- Edges come from graphify's AST extraction; descriptions and defect flags were curated by hand.

## Regenerating

Run these from the repository root:

```bash
python3 docs/code-graph/s3api/tools/extract.py <graphify-cache-dir> <ast-raw.json>
```

`extract.py` needs graphify installed, and it hardcodes `REPO = /home/user/seaweedfs`; change that line when you run it from another checkout. Run it without `-I`, because it imports the installed `graphify` module. Its seed lists (`NEIGH`, `FILE_PKGS`, `EXTRA_NEIGH`, `FILER_FILE_DIRS`, `FILER_COLLAPSED`, `FILER_SERVER_GLOB`) and `arrange.py`'s `FILE_PKGS` / `COLLAPSED_EXTRA` / `FILER_FILE_DIRS` / `GENERATED` must agree.

```bash
python3 -I docs/code-graph/s3api/tools/arrange.py <ast-raw.json> docs/code-graph/s3api/layers.json docs/code-graph/s3api/data.json
```

```bash
python3 -I docs/code-graph/s3api/tools/assemble.py docs/code-graph/s3api .
```

`merge_curation.py` adds new curation fields to `curation.json` but never overwrites existing ones; it reports conflicts instead. `assemble.py` fills `viewer-template.html` and reports:

- curation keys with no node
- nodes whose file no longer exists
- uncurated nodes
- lane mismatches

All four must be zero before publishing.

After the code changes, re-run extract and arrange, then run assemble. Any node it reports as uncurated, or whose source changed, needs to be read again before its description can be trusted.
