// connection-state archives stopped state or restores an inspection-only copy.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/connections"
)

func main() {
	directory := flag.String("state-dir", "", "stopped source directory for backup, new inspection directory for restore")
	backup := flag.String("backup", "", "new private archive path; never overwrite")
	restore := flag.String("restore", "", "existing archive to restore for inspection only")
	flag.Parse()
	if *directory == "" || (*backup == "") == (*restore == "") {
		fmt.Fprintln(os.Stderr, "state directory and exactly one backup/restore path required")
		os.Exit(1)
	}
	var out *connections.StateArchive
	var err error
	if *backup != "" {
		out, err = connections.ArchiveState(*directory, *backup)
	} else {
		out, err = connections.RestoreState(*restore, *directory)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "state archive operation failed; preserve originals and inspect:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if encoder.Encode(out) != nil {
		os.Exit(1)
	}
}
