package s3tables

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/require"
)

// A table bucket policy may condition on the bucket's own tags:
// s3tables:TableBucketTag/<key> and aws:ResourceTag/<key> are condition keys
// validatePolicyDocument accepts, so a statement written with one of them must
// hold on every operation on the bucket, the same way it already holds on
// PutTableBucketPolicy and on the namespace and table operations.

const (
	tagDenyBucket = "tagdenybkt"
	tagDenyWriter = "writer"
	tagDenyOther  = "stranger"
)

// bucketTagConditionKeys are the two keys that name a tag of the bucket the
// request acts on.
var bucketTagConditionKeys = []string{"s3tables:TableBucketTag/env", "aws:ResourceTag/env"}

// tagDenyCallers are the bucket owner and an account the bucket policy grants
// s3tables:*: a matching Deny vetoes both.
var tagDenyCallers = []string{DefaultAccountID, tagDenyWriter}

// tagDenyPolicy allows tagDenyWriter everything and denies everyone the bucket
// operations while condition holds. PutTableBucketPolicy is denied too: its
// handler already carries the bucket's tags, so it shows the condition works.
func tagDenyPolicy(condition string) string {
	return fmt.Sprintf(`{"Statement":[`+
		`{"Effect":"Allow","Principal":%q,"Action":"s3tables:*","Resource":"*"},`+
		`{"Effect":"Deny","Principal":"*","Action":["s3tables:GetTableBucket","s3tables:DeleteTableBucket","s3tables:PutTableBucketPolicy"],"Resource":"*",`+
		`"Condition":{"StringEquals":%s}}]}`, tagDenyWriter, condition)
}

// tagAllowPolicy allows tagDenyWriter everything only while condition holds.
func tagAllowPolicy(condition string) string {
	return fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Principal":%q,"Action":"s3tables:*","Resource":"*",`+
		`"Condition":{"StringEquals":%s}}]}`, tagDenyWriter, condition)
}

func tagCondition(key, value string) string {
	return fmt.Sprintf(`{%q:%q}`, key, value)
}

// startTaggedBucket creates an empty bucket with tags through the API, as its
// owner, checks the tags were stored, and puts policy on it.
func startTaggedBucket(t *testing.T, tags map[string]string, policy string) (*s3tablestest.MemFiler, *Manager, string) {
	t.Helper()
	fs := s3tablestest.Start(t)
	m := NewManager()
	client := NewManagerClient(fs.Client)

	var created CreateTableBucketResponse
	require.NoError(t, m.Execute(context.Background(), client, "CreateTableBucket", &CreateTableBucketRequest{
		Name: tagDenyBucket, Tags: tags,
	}, &created, DefaultAccountID))
	bucketARN := created.ARN
	require.NotEmpty(t, bucketARN)

	var stored ListTagsForResourceResponse
	require.NoError(t, m.Execute(context.Background(), client, "ListTagsForResource", &ListTagsForResourceRequest{
		ResourceARN: bucketARN,
	}, &stored, DefaultAccountID))
	for key, value := range tags {
		require.Equal(t, value, stored.Tags[key], "the bucket must carry tag %s", key)
	}

	require.NoError(t, m.Execute(context.Background(), client, "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: policy,
	}, nil, DefaultAccountID), "validatePolicyDocument accepts the policy")
	return fs, m, bucketARN
}

// startBucketTagDeny tags the bucket env=prod and denies the bucket operations
// while env is prod. The owner being refused PutTableBucketPolicy shows the
// policy, the tag and the condition all work, so only the context the tested
// handler builds can let the Deny through.
func startBucketTagDeny(t *testing.T, conditionKey string) (*s3tablestest.MemFiler, *Manager, string) {
	t.Helper()
	policy := tagDenyPolicy(tagCondition(conditionKey, "prod"))
	fs, m, bucketARN := startTaggedBucket(t, map[string]string{"env": "prod"}, policy)
	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "PutTableBucketPolicy", &PutTableBucketPolicyRequest{
		TableBucketARN: bucketARN, ResourcePolicy: policy,
	}, nil, DefaultAccountID)
	requireS3TablesError(t, err, ErrCodeAccessDenied)
	return fs, m, bucketARN
}

func forEachBucketTagDeny(t *testing.T, run func(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string)) {
	for _, key := range bucketTagConditionKeys {
		for _, caller := range tagDenyCallers {
			t.Run(key+"/"+caller, func(t *testing.T) {
				fs, m, bucketARN := startBucketTagDeny(t, key)
				run(t, fs, m, bucketARN, caller)
			})
		}
	}
}

func getTableBucketAs(fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string) (GetTableBucketResponse, error) {
	var resp GetTableBucketResponse
	err := m.Execute(context.Background(), NewManagerClient(fs.Client), "GetTableBucket", &GetTableBucketRequest{
		TableBucketARN: bucketARN,
	}, &resp, caller)
	return resp, err
}

func bucketListedTo(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, caller string) bool {
	t.Helper()
	var resp ListTableBucketsResponse
	require.NoError(t, m.Execute(context.Background(), NewManagerClient(fs.Client), "ListTableBuckets",
		&ListTableBucketsRequest{}, &resp, caller))
	for _, bucket := range resp.TableBuckets {
		if bucket.Name == tagDenyBucket {
			return true
		}
	}
	return false
}

func deleteTableBucketAs(fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string) error {
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "DeleteTableBucket", &DeleteTableBucketRequest{
		TableBucketARN: bucketARN,
	}, nil, caller)
}

// TestBucketTagDenyHoldsOnGetTableBucket: GetTableBucket hides a bucket the
// caller may not read, so the Deny answers NoSuchBucket.
func TestBucketTagDenyHoldsOnGetTableBucket(t *testing.T) {
	forEachBucketTagDeny(t, func(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string) {
		resp, err := getTableBucketAs(fs, m, bucketARN, caller)
		require.Error(t, err, "the bucket tag Deny was skipped: GetTableBucket returned %q", resp.Name)
		requireS3TablesError(t, err, ErrCodeNoSuchBucket)
	})
}

// TestBucketTagDenyHoldsOnListTableBuckets: ListTableBuckets lists a bucket
// only to a caller that may get it, so a denied bucket is left out.
func TestBucketTagDenyHoldsOnListTableBuckets(t *testing.T) {
	forEachBucketTagDeny(t, func(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string) {
		require.False(t, bucketListedTo(t, fs, m, caller), "the bucket tag Deny was skipped: the bucket is listed")
	})
}

// TestBucketTagDenyHoldsOnDeleteTableBucket: the Deny refuses the delete and
// the bucket stays.
func TestBucketTagDenyHoldsOnDeleteTableBucket(t *testing.T) {
	forEachBucketTagDeny(t, func(t *testing.T, fs *s3tablestest.MemFiler, m *Manager, bucketARN, caller string) {
		err := deleteTableBucketAs(fs, m, bucketARN, caller)
		require.NotNil(t, fs.Get(TablesPath, tagDenyBucket), "the bucket tag Deny was skipped: the bucket was deleted")
		require.Error(t, err)
		requireS3TablesError(t, err, ErrCodeNoSuchBucket)
	})
}

// TestBucketTagAllowGrantsBucketOps: the same missing context kept an Allow
// conditioned on the bucket's tag from granting, so a grantee could not get,
// list or delete the bucket the policy gives it.
func TestBucketTagAllowGrantsBucketOps(t *testing.T) {
	for _, key := range bucketTagConditionKeys {
		t.Run(key, func(t *testing.T) {
			policy := tagAllowPolicy(tagCondition(key, "prod"))
			tags := map[string]string{"env": "prod"}

			fs, m, bucketARN := startTaggedBucket(t, tags, policy)
			resp, err := getTableBucketAs(fs, m, bucketARN, tagDenyWriter)
			require.NoError(t, err, "the bucket tag Allow must grant GetTableBucket")
			require.Equal(t, tagDenyBucket, resp.Name)
			require.True(t, bucketListedTo(t, fs, m, tagDenyWriter), "the bucket tag Allow must list the bucket")

			require.NoError(t, deleteTableBucketAs(fs, m, bucketARN, tagDenyWriter), "the bucket tag Allow must grant DeleteTableBucket")
			require.Nil(t, fs.Get(TablesPath, tagDenyBucket))
		})
	}
}
