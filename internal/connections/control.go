package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
)

type Control struct {
	URL, ID, Token string
	Client         *http.Client
}

func NewControl(origin, id, token string) (*Control, error) {
	if !protocol.Origin(origin) || !protocol.Identifier.MatchString(id) || len(token) < 32 {
		return nil, errors.New("regional HTTPS origin, connection ID and scoped worker credential required")
	}
	return &Control{origin, id, token, protocol.HTTPClient()}, nil
}
func (c *Control) request(ctx context.Context, method, kind string, body any) ([]byte, int, error) {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, 0, errors.New("invalid regional metadata")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+"/v1/connections/"+c.ID+"/"+kind, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, errors.New("invalid regional request")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, 0, errors.New("regional control unavailable")
	}
	defer res.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(res.Body, protocol.MaxBody+1))
	if err != nil || len(raw) > protocol.MaxBody {
		return nil, res.StatusCode, errors.New("invalid regional metadata")
	}
	return raw, res.StatusCode, nil
}
func (c *Control) Desired(ctx context.Context) (protocol.Approved, error) {
	var a protocol.Approved
	if err := ctx.Err(); err != nil {
		return a, err
	}
	raw, status, err := c.request(ctx, "GET", "desired", nil)
	if ctx.Err() != nil {
		return a, ctx.Err()
	}
	if err != nil || status != 200 || protocol.Decode(bytes.NewReader(raw), &a) != nil || a.Binding.ConnectionID != c.ID || a.Validate(a.Binding.Region) != nil {
		return a, errors.New("regional command unavailable or invalid")
	}
	return a, nil
}
func (c *Control) Observed(ctx context.Context) (*protocol.Observed, error) {
	raw, status, err := c.request(ctx, "GET", "observed", nil)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, nil
	}
	var o protocol.Observed
	if status != 200 || protocol.Decode(bytes.NewReader(raw), &o) != nil || !o.Observation.Validate(c.ID) {
		return nil, errors.New("regional observation unavailable or invalid")
	}
	return &o, nil
}
func (c *Control) Report(ctx context.Context, o protocol.Observation) error {
	_, status, err := c.request(ctx, "PUT", "observed", o)
	if err != nil {
		return err
	}
	if status != 204 {
		return errors.New("regional observation not accepted")
	}
	return nil
}
