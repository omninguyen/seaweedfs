# weed/s3api code graph

A file-level map of the SeaweedFS S3 gateway. The structure comes from the AST and the explanations were written by hand. Open `s3api-graph.html` in a browser. It is self-contained, with no server or network needed.

## What is in it

- **109 nodes:** the 98 non-test files of package `weed/s3api`, plus 11 neighbor packages it imports, each shown as one collapsed node. The neighbor packages are `s3err`, `s3_constants`, `policy_engine`, `s3tables`, `s3lifecycle` (top-level files only), `s3lifecycle/scheduler`, `lifecycle_xml`, `s3bucket`, `cors`, `s3_objectlock` and `policy`.
- **928 edges,** extracted with graphify and aggregated from symbols to files. Edge types are `calls`, `indirect_call` (function values such as route registration), `references`, `embeds` and `imports_from`. No edge was added by hand.
- **8 lanes, ordered by request flow:**

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

- **Excluded:** `*_test.go`, test helper packages (`s3tablestest`, `lifecycletest`, `iceberg/testdata`), `policy_engine/examples.go` and `kms_azure.go`.

Each node was curated by reading its source in full. The panel shows these fields:

| Field | Content |
| --- | --- |
| character | One-line role |
| what | What the code does |
| why | The consequence if it fails, the design decision behind it, and any AWS compatibility constraint |
| when | When it runs |
| ux | What users see |
| if_broken | What breaks if it does |
| gotchas | Non-obvious behavior to know before changing it |
| issue / issue_note | The defect flag and its explanation, where one applies |

## Flagged defects (20)

"Probed" means the defect was reproduced with a throwaway test that was then deleted. "Read" means it was found by reading the code and is not reproduced.

| Kind | Node | Summary | Check |
| --- | --- | --- | --- |
| bug | `s3err/` | `ErrMalformedCredentialDate` has no response entry, so `WriteErrorResponse` panics (`WriteHeader(0)`); reachable without authentication. | probed |
| bug | `chunked_reader_v4.go` | STREAMING-…-TRAILER uploads: chunk and trailer signatures are not enforced, so the body is unauthenticated. | probed |
| bug | `policy_engine/` | `s3:authType` is not set for SigV4 presigned URLs, so "deny presigned" policies do not apply to them. | probed |
| bug | `s3api_embedded_iam.go` | `CreatePolicyVersion` rejects Deny / non-s3 documents that `CreatePolicy` accepts, so such policies can never be updated (Terraform). | probed |
| bug | `s3tables/` | After a cross-namespace `RenameTable`, every Iceberg commit fails ("metadata location must be within namespace"). | probed |
| bug | `cors/` | A preflight uses the first rule whose origin matches even when its methods don't, so a later rule that allows PUT is never used. | probed |
| bug | `s3bucket/` | `VerifyS3BucketName` accepts non-ASCII letters and digits ("bücket"), and CreateBucket relies on it alone. | probed |
| bug | `auth_signature_v2.go` | Header-signed SigV2 has no Date freshness check (replay), and several subresources are missing from the signed resource. | read |
| bug | `s3api_put_handlers.go` | SSE-S3 multipart parts reuse the AES-CTR keystream (shared DEK and base IV, part offset 0). | read |
| bug | `s3api_object_handlers_multipart.go` | `isVersioningEnabled` is passed as `objectLockEnabled`: lock headers are accepted on non-lock buckets and are not enforced. | read |
| bug | `s3api_object_handlers_put.go` | An invalid storage class is rejected only after the upload, and the uploaded chunks leak. | read |
| bug | `s3api_object_handlers_copy.go` | Copied chunks leak when finalize fails (for example a 412); UploadPartCopy misses `prefixObjectSource`. | read |
| bug | `s3api_bucket_policy_handlers.go` | Unconditional whole-entry bucket rewrite: a concurrent versioning or Object Lock config write can be reverted. | read |
| bug | `s3api_server.go` | `ValidateS3Credential` compares secrets with `!=` (a timing side channel on the OAuth token endpoints); low severity. | read |
| dead | `s3_sse_kms.go` | The S3 Bucket Key data-key cache path is not wired in. | read |
| dead | `s3api_copy_size_calculation.go` | Its only output is a V(2) log line. | read |
| dead | `filer_util_tags.go` | `getTags` has no callers. | read |
| duplicate | `s3api_object_handlers.go` | GET and HEAD each carry a ~90-line copy of versioned entry resolution. | read |
| duplicate | `filer_multipart.go` | Three near-identical decorate closures in `completeMultipartUpload`. | read |
| duplicate | `s3lifecycle/` | `version_time.go` copies the gateway's version-ID comparator. | read |

Use the **Defects** chips in the viewer to show only flagged nodes.

## Provenance

- Source: branch `claude/brave-carson-13bjg1` at base commit `984eac6`. `weed/` is unchanged since then; this folder is the only addition.
- Edges come from graphify's AST extraction; descriptions and defect flags were curated by hand.

## Regenerating

Run these from the repository root:

```bash
python3 docs/code-graph/s3api/tools/extract.py <graphify-cache-dir> <ast-raw.json>
```

`extract.py` needs graphify installed, and it hardcodes `REPO = /home/user/seaweedfs`; change that line when you run it from another checkout. Run it without `-I`, because it imports the installed `graphify` module.

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
