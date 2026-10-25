package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

func main() {
	configPath := flag.String("config", "/config/binding.json", "approved regional configuration bundle")
	statePath := flag.String("state", "/state/checkpoint.db", "exclusive regional state database")
	controlURL := flag.String("control-url", "", "regional control HTTPS origin; enables managed mode")
	bindingID := flag.String("binding-id", "", "managed binding identity")
	controlTokenEnv := flag.String("control-token-env", "CDC_CONTROL_TOKEN", "binding-scoped regional credential reference")
	searchListen := flag.String("search-listen", "", "optional regional search TLS listener; managed mode only")
	searchTokenEnv := flag.String("search-token-env", "CDC_SEARCH_TOKEN", "static local-spike search credential reference; ignored with signed grants")
	searchCert := flag.String("search-tls-cert", "", "regional search TLS certificate")
	searchKey := flag.String("search-tls-key", "", "regional search TLS private key")
	searchKeys := flag.String("search-verification-keys", "", "local Ed25519 JWKS file; enables signed grants and disables static-token authentication")
	flag.Parse()
	var err error
	if *controlURL != "" {
		err = managed(*controlURL, *bindingID, *controlTokenEnv, *statePath, searchOptions{listen: *searchListen, tokenEnv: *searchTokenEnv, cert: *searchCert, key: *searchKey, verificationKeys: *searchKeys})
	} else if *searchListen != "" {
		err = fmt.Errorf("regional search requires managed control")
	} else {
		err = run(*configPath, *statePath)
	}
	if err != nil {
		// Driver/API errors can include credentials or source values. Report only
		// a redacted terminal status here; tests inspect typed/local failures.
		fmt.Fprintln(os.Stderr, "CDC stopped; verify authority, source continuity, regional state and target generation before recovery")
		os.Exit(1)
	}
}

func managed(url, id, tokenEnv, statePath string, options searchOptions) error {
	remote, err := postgrescdc.NewControlClient(url, id, os.Getenv(tokenEnv))
	if err != nil {
		return err
	}
	store, err := postgrescdc.OpenStore(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	gate, stopSearch, err := startSearch(ctx, cancel, store, id, remote.Token, options)
	if err != nil {
		return err
	}
	defer stopSearch()
	supervisor := postgrescdc.Supervisor{Store: store, Remote: remote, Serving: gate, Report: func(p postgrescdc.Progress) { _ = json.NewEncoder(os.Stdout).Encode(p) }}
	err = supervisor.Run(ctx)
	if searchErr := stopSearch(); searchErr != nil {
		return searchErr
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}
func run(configPath, statePath string) error {
	file, err := os.Open(configPath)
	if err != nil {
		return err
	}
	defer file.Close()
	var c postgrescdc.Config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&c); err != nil {
		return err
	}
	if err = c.Validate(); err != nil {
		return err
	}
	dsn, key := os.Getenv(c.SourceDSNEnv), os.Getenv(c.TargetKeyEnv)
	if dsn == "" || key == "" {
		return fmt.Errorf("configured secret references are unavailable")
	}
	store, err := postgrescdc.OpenStore(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	w := postgrescdc.Worker{Config: c, Store: store, Target: postgrescdc.NewLambdaTarget(c, key), Report: func(p postgrescdc.Progress) { _ = json.NewEncoder(os.Stdout).Encode(p) }}
	err = w.Run(ctx, dsn)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
