package statefile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Runs only in an owned qualification task with the actual connection worker
// role. Every mutation uses a fresh, unreferenced canary, never a live head/log.
func TestLiveS3StatePermissions(t *testing.T) {
	if os.Getenv("CONNECTION_TEST_S3_PERMISSIONS") != "1" {
		t.Skip("explicit isolated worker-role S3 qualification required")
	}
	region, prefix, id := os.Getenv("AWS_REGION"), os.Getenv("CONNECTION_TEST_S3_PREFIX"), os.Getenv("CONNECTION_TEST_S3_CONNECTION")
	if region != "ap-northeast-2" || !ValidatePrefix(prefix) || !connectionID.MatchString(id) {
		t.Fatal("pinned Seoul qualification identity required")
	}
	objects, err := NewS3Objects(t.Context(), region, os.Getenv("CONNECTION_TEST_S3_BUCKET"))
	if err != nil {
		t.Fatal("qualification client unavailable")
	}
	raw, _, err := objects.Get(t.Context(), prefix+"store.json", 4096)
	var marker StoreIdentity
	if err != nil || json.Unmarshal(raw, &marker) != nil || marker.Format != 1 || marker.Region != region {
		t.Fatal("worker could not read its pinned store marker")
	}
	nonce, err := Token()
	if err != nil {
		t.Fatal(err)
	}
	key := prefix + "workers/" + id + "/records/permission-" + nonce
	etag, err := objects.Put(t.Context(), key, []byte("permission canary"), "")
	if err != nil {
		t.Fatal("own-prefix conditional creation denied")
	}
	expectDenied := func(label string, err error) {
		t.Helper()
		var api smithy.APIError
		if !errors.As(err, &api) || api.ErrorCode() != "AccessDenied" {
			t.Fatal(label + ": an exact AccessDenied response was required")
		}
		t.Log(label + ": denied")
	}
	_, err = objects.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(objects.bucket), Key: aws.String(key),
		Body: bytes.NewReader([]byte("must not overwrite")), IfMatch: aws.String(etag)})
	expectDenied("immutable record overwrite", err)
	_, err = objects.client.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: aws.String(objects.bucket), Key: aws.String(key)})
	expectDenied("runtime state deletion", err)
	_, err = objects.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(objects.bucket),
		Key: aws.String(prefix + "workers/" + id + "/permission-unconditional-" + nonce), Body: bytes.NewReader([]byte("canary"))})
	expectDenied("unconditional state write", err)
	_, err = objects.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(objects.bucket),
		Key: aws.String(prefix + "workers/" + id + "-other/records/permission-" + nonce), Body: bytes.NewReader([]byte("canary")), IfNoneMatch: aws.String("*")})
	expectDenied("foreign connection write", err)
	// This known, nonsecret object is seeded by the qualification runner.
	out, err := objects.client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(objects.bucket),
		Key: aws.String(prefix + "workers/" + id + "-other/permission-read-probe")})
	if out != nil && out.Body != nil {
		out.Body.Close()
	}
	expectDenied("foreign connection read", err)
	controlID := "permission-control-" + nonce
	control, _ := json.Marshal(map[string]any{"format": 1, "store_id": marker.ID, "region": region, "connection_id": controlID,
		"revision": 1, "buckets": map[string]any{"connections": map[string]any{}, "coordination": map[string]any{}, "connection_jobs": map[string]any{}}})
	_, err = objects.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(objects.bucket),
		Key: aws.String(prefix + "control/" + controlID), Body: bytes.NewReader(control), IfNoneMatch: aws.String("*")})
	expectDenied("regional control write", err)
	_, err = objects.client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String(objects.bucket), Prefix: aws.String(prefix + "control/"), MaxKeys: aws.Int32(1)})
	expectDenied("regional control discovery", err)
}
