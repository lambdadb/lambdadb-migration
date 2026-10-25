package connections

import (
	"context"
	"encoding/json"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	"net/url"
	"testing"
)

func TestCredentialReferencesAndTLSFailBeforeNetwork(t *testing.T) {
	resolver := AWSSecrets{}
	for _, ref := range []string{"env:SOURCE", "arn:aws:secretsmanager:us-west-2:123:secret:source", "https://secret"} {
		if _, err := resolver.Resolve(context.Background(), "ap-northeast-2", ref); err == nil {
			t.Fatal("unsafe regional secret resolved")
		}
	}
	raw := `{"host":"localhost","port":5432,"database":"test","username":"user","password":"p@ss:/word","ssl_mode":"disable"}`
	if _, err := SourceDSN(raw, false); err == nil {
		t.Fatal("plaintext source accepted by default")
	}
	dsn, err := SourceDSN(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if password != "p@ss:/word" {
		t.Fatal("password URL escaping changed credential")
	}
}
func TestChangedLargeIntegerIsNotDroppedAsEqual(t *testing.T) {
	if equalRow(mapRow(`9007199254740992`), mapRow(`9007199254740993`)) {
		t.Fatal("large integer source update collapsed")
	}
	if !equalRow(mapRow(`{"a":1,"b":2}`), mapRow(`{ "b":2, "a":1 }`)) {
		t.Fatal("object order changed row equality")
	}
}

func mapRow(raw string) postgrescdc.Row { return postgrescdc.Row{"value": json.RawMessage(raw)} }
