package s3tables

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These pin what GetTableBucket, ListTableBuckets and DeleteTableBucket answer
// on master wherever the bucket's tags do not satisfy the condition, so
// evaluating tag conditions on them changes nothing else. They pass with and
// without that change.

// TestBucketTagDenySparesOtherTagValues: a Deny on env=prod leaves a bucket
// tagged env=dev to its owner and grantee.
func TestBucketTagDenySparesOtherTagValues(t *testing.T) {
	requireBucketOpsAllowed(t, map[string]string{"env": "dev"}, func(key string) string {
		return tagDenyPolicy(tagCondition(key, "prod"))
	})
}

// TestBucketTagDenySparesUntaggedBucket: a Deny on a tag the bucket does not
// carry does not match.
func TestBucketTagDenySparesUntaggedBucket(t *testing.T) {
	requireBucketOpsAllowed(t, nil, func(key string) string {
		return tagDenyPolicy(tagCondition(key, "prod"))
	})
}

// requireBucketOpsAllowed: with the bucket tagged tags and the policy for each
// condition key, the owner and tagDenyWriter can get, list and delete it.
func requireBucketOpsAllowed(t *testing.T, tags map[string]string, policyFor func(key string) string) {
	t.Helper()
	for _, key := range bucketTagConditionKeys {
		for _, caller := range tagDenyCallers {
			t.Run(key+"/"+caller, func(t *testing.T) {
				fs, m, bucketARN := startTaggedBucket(t, tags, policyFor(key))
				resp, err := getTableBucketAs(fs, m, bucketARN, caller)
				require.NoError(t, err)
				require.Equal(t, tagDenyBucket, resp.Name)
				require.True(t, bucketListedTo(t, fs, m, caller))
				require.NoError(t, deleteTableBucketAs(fs, m, bucketARN, caller))
				require.Nil(t, fs.Get(TablesPath, tagDenyBucket))
			})
		}
	}
}

// TestBucketTagAllowSparesOtherTagValues: an Allow on env=prod grants nothing
// on a bucket tagged env=dev; the bucket reads as missing to the grantee.
func TestBucketTagAllowSparesOtherTagValues(t *testing.T) {
	for _, key := range bucketTagConditionKeys {
		t.Run(key, func(t *testing.T) {
			fs, m, bucketARN := startTaggedBucket(t, map[string]string{"env": "dev"}, tagAllowPolicy(tagCondition(key, "prod")))
			_, err := getTableBucketAs(fs, m, bucketARN, tagDenyWriter)
			requireS3TablesError(t, err, ErrCodeNoSuchBucket)
			require.False(t, bucketListedTo(t, fs, m, tagDenyWriter))
			requireS3TablesError(t, deleteTableBucketAs(fs, m, bucketARN, tagDenyWriter), ErrCodeNoSuchBucket)
			require.NotNil(t, fs.Get(TablesPath, tagDenyBucket))
		})
	}
}

// TestBucketNameDenyStillHoldsOnBucketOps: a Deny on s3tables:tableBucketName,
// a key these handlers already supply, keeps refusing the owner and grantee.
func TestBucketNameDenyStillHoldsOnBucketOps(t *testing.T) {
	for _, caller := range tagDenyCallers {
		t.Run(caller, func(t *testing.T) {
			fs, m, bucketARN := startTaggedBucket(t, nil, tagDenyPolicy(tagCondition("s3tables:tableBucketName", tagDenyBucket)))
			_, err := getTableBucketAs(fs, m, bucketARN, caller)
			requireS3TablesError(t, err, ErrCodeNoSuchBucket)
			require.False(t, bucketListedTo(t, fs, m, caller))
			requireS3TablesError(t, deleteTableBucketAs(fs, m, bucketARN, caller), ErrCodeNoSuchBucket)
			require.NotNil(t, fs.Get(TablesPath, tagDenyBucket))
		})
	}
}

// TestUnconditionalBucketPolicyUnchanged: a plain Allow lets the grantee and
// the owner in and leaves anyone else out.
func TestUnconditionalBucketPolicyUnchanged(t *testing.T) {
	policy := `{"Statement":[{"Effect":"Allow","Principal":"` + tagDenyWriter + `","Action":"s3tables:*","Resource":"*"}]}`
	fs, m, bucketARN := startTaggedBucket(t, map[string]string{"env": "prod"}, policy)
	for _, caller := range tagDenyCallers {
		_, err := getTableBucketAs(fs, m, bucketARN, caller)
		require.NoError(t, err, caller)
		require.True(t, bucketListedTo(t, fs, m, caller), caller)
	}
	_, err := getTableBucketAs(fs, m, bucketARN, tagDenyOther)
	requireS3TablesError(t, err, ErrCodeNoSuchBucket)
	require.False(t, bucketListedTo(t, fs, m, tagDenyOther))
	requireS3TablesError(t, deleteTableBucketAs(fs, m, bucketARN, tagDenyOther), ErrCodeNoSuchBucket)
	require.NotNil(t, fs.Get(TablesPath, tagDenyBucket))
	require.NoError(t, deleteTableBucketAs(fs, m, bucketARN, tagDenyWriter))
}
