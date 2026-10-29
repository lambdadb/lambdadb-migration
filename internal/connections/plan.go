package connections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// ApplicationPlan retains completion even after the source checkpoint advances.
// Acceptance and cursor advancement share one journal transaction.
type ApplicationPlan struct {
	ID     string `json:"id"`
	Cursor uint64 `json:"cursor"`
	Total  uint64 `json:"total"`
}

type targetOperation struct {
	Operation string          `json:"operation"`
	Content   json.RawMessage `json:"content"`
}

func validatePlan(s State) error {
	if s.Plan != nil {
		id, err := hex.DecodeString(s.Plan.ID)
		if s.Format == 3 || err != nil || len(id) != sha256.Size || s.Plan.Cursor > s.Plan.Total || s.Plan.Total > 2*MaxManagedRows {
			return errors.New("invalid application plan")
		}
	}
	if p := s.Pending; p != nil {
		if p.PlanID == "" {
			if p.PlanIndex != 0 || (s.Plan != nil && s.Plan.Cursor != s.Plan.Total) {
				return errors.New("unbound target intent in application plan")
			}
		} else if s.Plan == nil || s.Plan.ID != p.PlanID || s.Plan.Cursor != p.PlanIndex || p.PlanIndex >= s.Plan.Total {
			return errors.New("target intent does not match application plan")
		}
	}
	return nil
}

func (j *Journal) beginPlan(id string, total uint64) (uint64, error) {
	var cursor uint64
	err := j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		if s.Pending != nil {
			return ErrPending
		}
		if s.Plan != nil && s.Plan.ID == id {
			if s.Plan.Total != total {
				return errors.New("application plan changed")
			}
			cursor = s.Plan.Cursor
			return errUnchanged
		}
		if s.Plan != nil && s.Plan.Cursor != s.Plan.Total {
			return errors.New("application plan is incomplete")
		}
		s.Plan = &ApplicationPlan{ID: id, Total: total}
		if err := validatePlan(*s); err != nil {
			return err
		}
		return j.write(tx, s)
	})
	if errors.Is(err, errUnchanged) {
		err = nil
	}
	return cursor, err
}

// applyPlan never reconstructs a new delivery for an already accepted item.
// The caller must retain the source data used to derive these exact operations.
func (j *Journal) applyPlan(ctx context.Context, target IntentTarget, identity string, operations []targetOperation) error {
	h := sha256.New()
	encoder := json.NewEncoder(h)
	if err := encoder.Encode(identity); err != nil {
		return err
	}
	for _, op := range operations {
		if err := encoder.Encode(op); err != nil {
			return err
		}
	}
	id := hex.EncodeToString(h.Sum(nil))
	cursor, err := j.beginPlan(id, uint64(len(operations)))
	if err != nil {
		return err
	}
	for index := cursor; index < uint64(len(operations)); index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		op := operations[index]
		intent, err := j.prepare(op.Operation, op.Content, id, index)
		if err != nil {
			return err
		}
		if err := j.deliver(ctx, target, *intent); err != nil {
			return err
		}
	}
	return nil
}
