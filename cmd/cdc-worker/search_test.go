package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSearchBootstrapRequiresIndependentCredentialAndTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate, stop, err := startSearch(ctx, cancel, nil, "binding", "control-token", searchOptions{})
	if err != nil || gate != nil {
		t.Fatal("disabled listener failed")
	}
	if err = stop(); err != nil {
		t.Fatal(err)
	}
	token := "synthetic-control-token-at-least-32-bytes"
	t.Setenv("TEST_SEARCH_TOKEN", token)
	if _, _, err := startSearch(ctx, cancel, nil, "binding", token, searchOptions{listen: "127.0.0.1:0", tokenEnv: "TEST_SEARCH_TOKEN", cert: "missing.pem", key: "missing.pem"}); err == nil {
		t.Fatal("control credential reused for search")
	}
	t.Setenv("TEST_SEARCH_TOKEN", "separate-search-token-at-least-32-bytes")
	if _, _, err := startSearch(ctx, cancel, nil, "binding", token, searchOptions{listen: "127.0.0.1:0", tokenEnv: "TEST_SEARCH_TOKEN"}); err == nil {
		t.Fatal("non-TLS listener enabled")
	}
}

func TestInvalidSignedConfigurationCannotFallBackToStaticToken(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	key, err := x509.MarshalPKCS8PrivateKey(server.TLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SEARCH_TOKEN", "valid-legacy-search-token-at-least-32-bytes")
	options := searchOptions{listen: "127.0.0.1:0", tokenEnv: "TEST_SEARCH_TOKEN", cert: certPath, key: keyPath, verificationKeys: filepath.Join(dir, "missing.json")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if gate, _, err := startSearch(ctx, cancel, nil, "binding", "control", options); err == nil || gate != nil {
		t.Fatal("missing signed keys enabled a listener")
	}
	options.verificationKeys = filepath.Join(dir, "invalid.json")
	if err = os.WriteFile(options.verificationKeys, []byte(`{"keys":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if gate, _, err := startSearch(ctx, cancel, nil, "binding", "control", options); err == nil || gate != nil {
		t.Fatal("invalid signed keys fell back to static authentication")
	}
}
