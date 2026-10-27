// connection-worker executes one approved connection in its target region.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lambdadb/lambdadb-migration/internal/connections"
)

func main() {
	origin := flag.String("control-url", "", "regional HTTPS control origin")
	id := flag.String("connection-id", "", "approved connection ID")
	state := flag.String("state-dir", "", "retained per-connection regional state directory")
	region := flag.String("region", os.Getenv("AWS_REGION"), "worker region (defaults to approved target region)")
	initialize := flag.Bool("initialize", false, "one-time initialization for a newly registered connection")
	inspect := flag.Bool("inspect-state", false, "inspect a stopped worker's retained state read-only; never authorize resume")
	localSecrets := flag.Bool("allow-environment-secrets", false, "allow env references for isolated local testing")
	insecureSource := flag.Bool("allow-insecure-source", false, "allow plaintext PostgreSQL for isolated local testing")
	caFile := flag.String("ca-file", "", "additional trusted CA PEM for regional/target HTTPS")
	flag.Parse()
	if *inspect {
		if *initialize {
			fmt.Fprintln(os.Stderr, "inspection cannot initialize state")
			os.Exit(1)
		}
		out, err := connections.InspectState(*state)
		if err != nil {
			fmt.Fprintln(os.Stderr, "connection state inspection failed:", err)
			os.Exit(1)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err = encoder.Encode(out); err != nil {
			os.Exit(1)
		}
		return
	}
	control, err := connections.NewControl(*origin, *id, os.Getenv("CONNECTION_WORKER_CREDENTIAL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if *caFile != "" {
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		pem, e := os.ReadFile(*caFile)
		if e != nil || !roots.AppendCertsFromPEM(pem) {
			fmt.Fprintln(os.Stderr, "worker CA unavailable")
			os.Exit(1)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	control.Client.Transport = transport
	worker := connections.Worker{Control: control, Secrets: connections.AWSSecrets{AllowEnvironment: *localSecrets}, StateDirectory: *state, Region: *region, Initialize: *initialize, AllowInsecureSource: *insecureSource, Transport: transport}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "connection worker stopped:", err)
		os.Exit(1)
	}
}
