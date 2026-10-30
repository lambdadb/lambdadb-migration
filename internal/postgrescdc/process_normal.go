//go:build !cdctest

package postgrescdc

import "net/http"

func processBoundary(string)               {}
func configureProcessTestTLS(*http.Client) {}
