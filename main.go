package main

import "github.com/lambdadb/lambdadb-migration/cmd"

var (
	version = "dev"
	commit  = "none"
)

func main() {
	cmd.Execute(version, commit)
}
