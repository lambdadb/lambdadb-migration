package connections

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	sm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
)

type SecretResolver interface {
	Resolve(context.Context, string, string) (string, error)
}
type AWSSecrets struct{ AllowEnvironment bool }

func (s AWSSecrets) Resolve(ctx context.Context, region, ref string) (string, error) {
	if !protocol.CredentialReference(ref, region) {
		return "", errors.New("invalid regional credential reference")
	}
	if strings.HasPrefix(ref, "env:") {
		if !s.AllowEnvironment {
			return "", errors.New("environment credentials require explicit local-test option")
		}
		value := os.Getenv(strings.TrimPrefix(ref, "env:"))
		if value == "" || len(value) > protocol.MaxBody {
			return "", errors.New("local credential unavailable")
		}
		return value, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return "", errors.New("regional credentials unavailable")
	}
	out, err := sm.NewFromConfig(cfg).GetSecretValue(ctx, &sm.GetSecretValueInput{SecretId: aws.String(ref)})
	if err != nil || out.SecretString == nil || len(*out.SecretString) > protocol.MaxBody || *out.SecretString == "" {
		return "", errors.New("regional secret unavailable")
	}
	return *out.SecretString, nil
}

type PostgresCredentials struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
}

func SourceDSN(raw string, allowInsecure bool) (string, error) {
	var p PostgresCredentials
	if protocol.Decode(bytes.NewBufferString(raw), &p) != nil || p.Host == "" || len(p.Host) > 253 || strings.ContainsAny(p.Host, "/\\\x00\r\n\t ") || p.Port < 1 || p.Port > 65535 || p.Database == "" || len(p.Database) > 128 || p.Username == "" || len(p.Username) > 128 || p.Password == "" || len(p.Password) > 4096 {
		return "", errors.New("invalid PostgreSQL credentials")
	}
	if p.SSLMode != "verify-full" && p.SSLMode != "verify-ca" && !(allowInsecure && p.SSLMode == "disable") {
		return "", errors.New("verified PostgreSQL TLS required")
	}
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(p.Host, strconv.Itoa(p.Port)), Path: "/" + p.Database, User: url.UserPassword(p.Username, p.Password)}
	q := u.Query()
	q.Set("sslmode", p.SSLMode)
	q.Set("connect_timeout", "10")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
