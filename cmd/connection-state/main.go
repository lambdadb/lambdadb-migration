// connection-state archives stopped state or restores an inspection-only copy.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/connections"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
)

func main() {
	directory := flag.String("state-dir", "", "stopped source directory for backup, new inspection directory for restore")
	backup := flag.String("backup", "", "new private archive path; never overwrite")
	restore := flag.String("restore", "", "existing archive to restore for inspection only")
	initializeS3 := flag.Bool("initialize-s3-store", false, "create a new S3 store identity and exit; never overwrite")
	inspectS3 := flag.Bool("inspect-s3", false, "read the current S3 recovery state without claiming or resuming")
	bucket := flag.String("s3-bucket", "", "regional S3 recovery bucket")
	prefix := flag.String("s3-prefix", "", "deployment prefix ending in /")
	storeID := flag.String("s3-store-id", "", "pinned S3 store identity for inspection")
	region := flag.String("region", os.Getenv("AWS_REGION"), "S3 store region")
	id := flag.String("connection-id", "", "connection to inspect")
	fingerprint := flag.String("fingerprint", "", "approved binding fingerprint for inspection")
	flag.Parse()
	if *initializeS3 || *inspectS3 {
		if (*initializeS3 && *inspectS3) || *directory != "" || *backup != "" || *restore != "" || *bucket == "" || !statefile.ValidatePrefix(*prefix) || (*initializeS3 && (*storeID != "" || *id != "" || *fingerprint != "")) {
			fmt.Fprintln(os.Stderr, "invalid remote state operator options")
			os.Exit(1)
		}
		ctx := context.Background()
		objects, err := statefile.NewS3Objects(ctx, *region, *bucket)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var out any
		if *initializeS3 {
			out, err = statefile.InitializeStore(ctx, objects, *prefix, *region)
		} else {
			out, err = connections.InspectRemoteState(ctx, objects, *prefix, statefile.Binding{StoreID: *storeID, Region: *region, ConnectionID: *id, Fingerprint: *fingerprint})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "remote state operator action failed:", err)
			os.Exit(1)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encoder.Encode(out) != nil {
			os.Exit(1)
		}
		return
	}
	if *bucket != "" || *prefix != "" || *storeID != "" || *region != os.Getenv("AWS_REGION") || *id != "" || *fingerprint != "" {
		fmt.Fprintln(os.Stderr, "S3 options require an explicit remote operator action")
		os.Exit(1)
	}
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
