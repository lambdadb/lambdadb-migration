package connections

import (
	"context"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/statefile"
)

// InspectRemoteState validates the current S3 recovery head without changing
// ownership. A temporary materialization is inspection-only and never returned.
func InspectRemoteState(ctx context.Context, objects statefile.Objects, prefix string, binding statefile.Binding) (*Inspection, error) {
	r, err := statefile.OpenReplica(ctx, objects, prefix, binding, 1)
	if err != nil {
		return nil, err
	}
	files, err := r.Inspect()
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "connection-inspection-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(root)
	dir, err := statefile.Materialize(root, files)
	if err != nil {
		return nil, err
	}
	out, err := InspectState(dir)
	if err != nil {
		return nil, err
	}
	out.LocalStateOnly, out.RemoteStateOnly = false, true
	return out, nil
}
