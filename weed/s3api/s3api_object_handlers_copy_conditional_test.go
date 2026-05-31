package s3api

import (
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestValidateConditionalCopyHeadersUsesStoredMultipartETag(t *testing.T) {
	const multipartETag = "11111111111111111111111111111111-2"
	entry := &filer_pb.Entry{
		Name: "large-object",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 16 << 20,
		},
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte(multipartETag),
		},
	}
	s3a := &S3ApiServer{}

	req := httptest.NewRequest("PUT", "/bucket/final-object", nil)
	req.Header.Set(s3_constants.AmzCopySourceIfMatch, `"`+multipartETag+`"`)
	if errCode := s3a.validateConditionalCopyHeaders(req, entry); errCode != s3err.ErrNone {
		t.Fatalf("validateConditionalCopyHeaders with stored multipart ETag = %v, want %v", errCode, s3err.ErrNone)
	}

	req = httptest.NewRequest("PUT", "/bucket/final-object", nil)
	req.Header.Set(s3_constants.AmzCopySourceIfNoneMatch, multipartETag)
	if errCode := s3a.validateConditionalCopyHeaders(req, entry); errCode != s3err.ErrPreconditionFailed {
		t.Fatalf("validateConditionalCopyHeaders If-None-Match = %v, want %v", errCode, s3err.ErrPreconditionFailed)
	}
}
