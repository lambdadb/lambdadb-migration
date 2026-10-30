package statefile

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ConditionalHeadersAndUnknownResponseNeverRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := calls.Add(1)
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("request was not signed")
		}
		if index == 1 {
			if r.Header.Get("If-None-Match") != "*" || r.Header.Get("If-Match") != "" {
				t.Error("initial write was not create-only")
			}
			w.Header().Set("ETag", `"first"`)
			return
		}
		if r.Header.Get("If-Match") != `"first"` || r.Header.Get("If-None-Match") != "" {
			t.Error("update did not use the exact opaque ETag")
		}
		if index == 2 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(412)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			return
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	client := s3.New(s3.Options{Region: "ap-northeast-2", BaseEndpoint: aws.String(server.URL), UsePathStyle: true,
		Credentials:      aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("fixture", "fixture", "")),
		RetryMaxAttempts: 1, HTTPClient: server.Client()})
	objects := &S3Objects{client: client, bucket: "fixture"}
	etag, err := objects.Put(t.Context(), "head.json", []byte(`{}`), "")
	if err != nil || etag != `"first"` {
		t.Fatal(err)
	}
	if _, err := objects.Put(t.Context(), "head.json", []byte(`{}`), etag); !errors.Is(err, ErrConflict) {
		t.Fatal("conditional rejection misclassified", err)
	}
	if _, err := objects.Put(t.Context(), "head.json", []byte(`{}`), etag); err == nil || errors.Is(err, ErrConflict) || calls.Load() != 3 {
		t.Fatal("unknown response was retried or treated as non-acceptance", err, calls.Load())
	}
	metrics := objects.Metrics()
	if metrics.Puts != 3 || metrics.Gets != 0 || metrics.Failures != 2 || metrics.WriteBytes != 6 {
		t.Fatal("request metrics do not match actual SDK calls")
	}
}

func TestS3ErrorsCannotBecomeRecordAbsence(t *testing.T) {
	for _, test := range []struct {
		code   string
		status int
		absent bool
	}{
		{"NoSuchKey", 404, true}, {"NoSuchBucket", 404, false}, {"AccessDenied", 403, false}, {"InternalError", 500, false},
	} {
		t.Run(test.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(`<Error><Code>` + test.code + `</Code></Error>`))
			}))
			defer server.Close()
			client := s3.New(s3.Options{Region: "ap-northeast-2", BaseEndpoint: aws.String(server.URL), UsePathStyle: true,
				Credentials:      aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("fixture", "fixture", "")),
				RetryMaxAttempts: 1, HTTPClient: server.Client()})
			_, _, err := (&S3Objects{client: client, bucket: "fixture"}).Get(context.Background(), "head.json", 100)
			if err == nil || errors.Is(err, ErrNotFound) != test.absent {
				t.Fatal("S3 error became incorrect absence", err)
			}
		})
	}
}
