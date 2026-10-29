package statefile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3Objects struct {
	client *s3.Client
	bucket string
}

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var regionName = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`)

// NewS3Objects is called only after explicitly selecting S3 state. Local state
// execution never loads AWS configuration through this storage path.
func NewS3Objects(ctx context.Context, region, bucket string) (*S3Objects, error) {
	if !bucketName.MatchString(bucket) || !regionName.MatchString(region) {
		return nil, errors.New("invalid S3 state bucket or region")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errors.New("S3 state credentials unavailable")
	}
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		// A lost conditional-write response is not transparently retried.
		options.RetryMaxAttempts = 1
		options.HTTPClient = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	})
	return &S3Objects{client: client, bucket: bucket}, nil
}

func objectError(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey":
			return ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrConflict
		}
	}
	return errors.New("S3 state request failed or unconfirmed")
}

func (s *S3Objects) Get(ctx context.Context, key string, limit int64) ([]byte, string, error) {
	if limit <= 0 || limit > maxObjectBytes {
		return nil, "", errors.New("invalid state object read limit")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", objectError(err)
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(out.Body, limit+1))
	if err != nil || int64(len(raw)) > limit || aws.ToString(out.ETag) == "" {
		return nil, "", errors.New("S3 state object unavailable or oversized")
	}
	return raw, aws.ToString(out.ETag), nil
}

func (s *S3Objects) Put(ctx context.Context, key string, raw []byte, etag string) (string, error) {
	if len(raw) == 0 || len(raw) > maxObjectBytes {
		return "", errors.New("invalid state object write size")
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(raw), ContentType: aws.String("application/octet-stream")}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(etag)
	}
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return "", objectError(err)
	}
	if aws.ToString(out.ETag) == "" {
		return "", errors.New("S3 state publication unconfirmed")
	}
	return aws.ToString(out.ETag), nil
}

// InitializeStore is an explicit operator action, separate from worker restart.
func InitializeStore(ctx context.Context, objects Objects, prefix, region string) (StoreIdentity, error) {
	if objects == nil || !ValidatePrefix(prefix) || !regionName.MatchString(region) {
		return StoreIdentity{}, errors.New("invalid S3 state store configuration")
	}
	id, err := Token()
	if err != nil {
		return StoreIdentity{}, err
	}
	marker := StoreIdentity{Format: 1, ID: id, Region: region}
	raw, _ := json.Marshal(marker)
	if _, err = objects.Put(ctx, prefix+"store.json", raw, ""); err != nil {
		return StoreIdentity{}, errors.New("store initialization unconfirmed; do not reinitialize")
	}
	return marker, nil
}
