package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

type searchOptions struct{ listen, tokenEnv, cert, key, verificationKeys string }

func startSearch(ctx context.Context, cancel context.CancelFunc, store *postgrescdc.Store, id, controlToken string, options searchOptions) (*postgrescdc.ServingGate, func() error, error) {
	if options.listen == "" {
		return nil, func() error { return nil }, nil
	}
	var token string
	if options.verificationKeys == "" {
		token = os.Getenv(options.tokenEnv)
	}
	if (options.verificationKeys == "" && (len(token) < 32 || token == controlToken)) || options.cert == "" || options.key == "" {
		return nil, nil, errors.New("search authentication and TLS certificate/key required")
	}
	certificate, err := tls.LoadX509KeyPair(options.cert, options.key)
	if err != nil {
		return nil, nil, errors.New("search TLS identity unavailable")
	}
	gate := postgrescdc.NewServingGate(store)
	var handler http.Handler
	if options.verificationKeys != "" {
		verifier, e := postgrescdc.LoadSearchGrantVerifier(options.verificationKeys)
		if e != nil {
			return nil, nil, e
		}
		handler, err = gate.SignedHandler(id, verifier)
	} else {
		handler, err = gate.Handler(id, token)
	}
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", options.listen)
	if err != nil {
		return nil, nil, errors.New("regional search listener unavailable")
	}
	listener = tls.NewListener(listener, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 4 * time.Second, IdleTimeout: 15 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan struct{})
	var serveErr error
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = errors.New("regional search server stopped")
			cancel()
		}
	}()
	stop := func() error {
		shutdown, stop := context.WithTimeout(context.Background(), 4*time.Second)
		defer stop()
		_ = server.Shutdown(shutdown)
		_ = server.Close()
		<-done
		return serveErr
	}
	return gate, stop, nil
}
