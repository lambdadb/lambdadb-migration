package pipeline

import "context"

type Runner struct{}

func (r Runner) Run(ctx context.Context) error {
	return nil
}
