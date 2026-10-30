//go:build cdctest

package postgrescdc

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"syscall"
)

// Only diagnostic binaries contain this deterministic stop mechanism. The parent
// sends a real SIGKILL; no cleanup handler or simulated Run error is involved.
func processBoundary(point string) {
	if os.Getenv("CDC_TEST_STOP_AT") != point {
		return
	}
	path := os.Getenv("CDC_TEST_BOUNDARY_FILE")
	if path == "" {
		panic("diagnostic boundary file missing")
	}
	if err := os.WriteFile(path, []byte(point), 0600); err != nil {
		panic("diagnostic boundary notification failed")
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGSTOP); err != nil {
		panic("diagnostic stop failed")
	}
}
func configureProcessTestTLS(client *http.Client) {
	path := os.Getenv("CDC_TEST_CA_FILE")
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		panic("diagnostic CA unavailable")
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(data) {
		panic("diagnostic CA invalid")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	client.Transport = transport
}
