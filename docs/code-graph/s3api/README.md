# weed/s3api code graph

A file-level map of the SeaweedFS S3 gateway and the catalog APIs that run beside it. The structure comes from the AST and the explanations were written by hand. Open `s3api-graph.html` in a browser. It is self-contained, with no server or network needed.

## What is in it

- **146 nodes:**
  - the 99 non-test files of package `weed/s3api`;
  - the 35 non-test files of the three catalog servers wired next to the gateway in `command/s3.go`: the Iceberg REST catalog (`weed/s3api/iceberg`, 21 files), the Lance Namespace REST server (`weed/s3api/lance`, 9) and the S3 Vectors API (`weed/s3api/s3vectors`, 5);
  - 12 collapsed packages, one node each: the 11 neighbor packages package `s3api` imports (`s3err`, `s3_constants`, `policy_engine`, `s3tables`, `s3lifecycle` (top-level files only), `s3lifecycle/scheduler`, `lifecycle_xml`, `s3bucket`, `cors`, `s3_objectlock`, `policy`) and the Iceberg maintenance worker `weed/worker/tasks/iceberg`.
- **1,103 edges** (820 distinct node pairs), extracted with graphify and aggregated from symbols to files. Edge types are `calls`, `indirect_call` (function values such as route registration), `references`, `embeds` and `imports_from`. No edge was added by hand.
- **12 lanes, ordered by request flow:**

| Lane | Contents |
| --- | --- |
| 0 Cross-cutting | Constants, error codes, XML types, metrics |
| 1 Server & routing | Server construction, routing, circuit breaker, path validation |
| 2 Auth & identity | SigV4/V2, IAM, STS, bucket policies, ACLs |
| 3 Bucket API | Bucket handlers and per-bucket config |
| 4 Object API | Object handlers |
| 5 Object features | Versioning, SSE, Object Lock, multipart, lifecycle TTL |
| 6 Filer access | Filer access |
| 7 Satellite APIs | S3 Tables, lifecycle RPC, SOSAPI |
| 8 Iceberg REST catalog | `weed/s3api/iceberg` |
| 9 Lance REST namespace | `weed/s3api/lance` |
| 10 S3 Vectors | `weed/s3api/s3vectors` |
| 11 Iceberg maintenance worker | `weed/worker/tasks/iceberg` (collapsed) |

- **Excluded:** `*_test.go`, test helper packages (`s3tablestest`, `lifecycletest`), `testdata` directories, `policy_engine/examples.go` and `kms_azure.go`.
- **A known gap in the edges:** the Iceberg, Lance and S3 Vectors servers register their routes with unexported method values (`s.handleX`), and graphify draws no edge for those. Their handler files therefore show no incoming edge from `server.go`. The gateway's own routes (`s3a.XHandler`) are drawn.

Each node was curated by reading its source in full. The collapsed worker node was curated from its handler, detection, commit, orphan and filer I/O code; its algorithm files (compaction, delete rewrite, row lineage, geometry) are summarized, not described one by one. The panel shows these fields:

| Field | Content |
| --- | --- |
| character | One-line role |
| what | What the code does |
| why | The consequence if it fails, the design decision behind it, and any compatibility constraint (AWS, Iceberg REST, Lance Namespace) |
| when | When it runs |
| ux | What users see |
| if_broken | What breaks if it does |
| gotchas | Non-obvious behavior to know before changing it |
| issue / issue_note | The defect flag and its explanation, where one applies |

## Flagged defects (28)

"Probed" means the defect was reproduced with a throwaway test that was then deleted. "Read" means it was found by reading the code and is not reproduced. Every bug row was checked again at master `b9866bae`: 20 were reproduced there, and the timing side channel in `s3api_server.go` was confirmed by reading.

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
| bug | `auth_signature_v2.go` | Header-signed SigV2 has no Date freshness check (replay), and several subresources are missing from the signed resource. | probed |
| bug | `s3api_object_handlers_multipart.go` | `isVersioningEnabled` is passed as `objectLockEnabled`: lock headers are accepted on non-lock buckets and are not enforced. | probed |
| bug | `s3api_object_handlers_copy.go` | Copied chunks leak when finalize fails (for example a 412); UploadPartCopy misses `prefixObjectSource`. | probed |
| bug | `s3api_bucket_policy_handlers.go` | Unconditional whole-entry bucket rewrite: a concurrent versioning or Object Lock config write can be reverted. | probed |
| bug | `s3api_server.go` | `ValidateS3Credential` compares secrets with `!=` (a timing side channel on the OAuth token endpoints); low severity. | read |
| bug | `iceberg/handlers_commit.go` | A non-conflict UpdateTable failure deletes the staged metadata file although the record may point at it. | probed |
| bug | `iceberg/handlers_transaction.go` | The rollback of a failed multi-table commit carries no version token and reverts commits other writers landed meanwhile. | probed |
| bug | `iceberg/handlers_view.go` | CreateView answers 200 with the stored definition for a view that already exists (the spec says 409), and with the same client-supplied location the loser of a concurrent create overwrites the winner's metadata file. | probed |
| bug | `lance/handlers_table.go` | Deregister and re-register change what every caller sees but require only read permission. | probed |
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

## Provenance

- Source: branch `claude/brave-carson-13bjg1`, with `weed/` equal to master `995084f`. This folder is the only addition.
- The 109 nodes of the first build were curated at `984eac6` and drift-checked to `199b2e2`. Seven files in that scope changed in between: `s3_metadata_util.go`, `s3api_object_handlers_multipart.go`, `s3api_object_handlers_put.go`, and four files of `s3tables/` (`handler_namespace.go`, `handler_policy.go`, `manager.go`, `permissions.go`). Each was re-read and its node updated (tag merging inside the compare, bucket binding for DeleteNamespace and tag operations, `s3vectors:VectorBucketTag/` condition keys, the storage-class fix).
- Second sync, `199b2e2` → `995084f`: two commits touched the scope. `af45ab8` changed only test files. `b0c6297` (per-bucket Block Public Access) added `s3api_public_access_block.go`, curated as a new node in lane 2, and changed nine files whose nodes were re-read and updated: `s3api_bucket_config.go`, `s3api_bucket_handlers.go`, `s3api_bucket_policy_handlers.go`, `s3api_object_anonymous_read.go`, `s3api_object_handlers_acl.go`, `s3api_object_handlers_postpolicy.go`, `s3api_object_upload_acl.go`, `s3_constants/` and `s3err/` (the existing flags on `s3err/` and `s3api_bucket_policy_handlers.go` still hold).
- The 36 nodes added for lanes 8–11 were curated at `199b2e2`; none of their files changed in the second sync.
- `iceberg/commit_helpers.go`, `iceberg/handlers_table.go` and `iceberg/metadata_files.go` were re-read at master `7506e5f` for the #12474 fix (and #12475's per-table directories), so those three nodes describe `7506e5f`. Every other node still describes `995084f`, and the edges are still extracted from `995084f`.
- Flag check at master `b9866bae` (120 commits after `995084f`, 25 of them in this scope): every bug flag was re-read and probed there, and the flags and issue notes of `s3tables/`, `iceberg/handlers_view.go`, `iceberg/handlers_commit.go`, `iceberg/handlers_transaction.go`, `lance/handlers_table.go`, `auth_signature_v2.go`, `s3api_object_handlers_multipart.go`, `s3api_object_handlers_copy.go`, `s3api_bucket_policy_handlers.go` and `worker/tasks/iceberg/` were updated. This was not a full sync: the other fields of every node, the node list and the edges still describe the commits named above.
- Edges come from graphify's AST extraction; descriptions and defect flags were curated by hand.

## Regenerating

Run these from the repository root:

```bash
python3 docs/code-graph/s3api/tools/extract.py <graphify-cache-dir> <ast-raw.json>
```

`extract.py` needs graphify installed, and it hardcodes `REPO = /home/user/seaweedfs`; change that line when you run it from another checkout. Run it without `-I`, because it imports the installed `graphify` module. Its seed lists (`NEIGH`, `FILE_PKGS`, `EXTRA_NEIGH`) and `arrange.py`'s `FILE_PKGS` / `COLLAPSED_EXTRA` must agree.

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
