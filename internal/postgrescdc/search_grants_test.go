package postgrescdc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func grantKeys(t *testing.T) (ed25519.PrivateKey, []byte) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"keys": []any{map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": "test-key", "x": base64.RawURLEncoding.EncodeToString(public), "alg": "EdDSA", "use": "sig"}}})
	return private, raw
}
func grantClaims(c Config, revision uint64, lifetime time.Duration) SearchGrantClaims {
	now := time.Now().UTC().Truncate(time.Second)
	return SearchGrantClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: searchGrantIssuer, Subject: "management-member", Audience: jwt.ClaimStrings{searchGrantAudience}, IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(lifetime)), ID: "request-id"}, ProtocolVersion: 1, BindingID: c.BindingID, BindingFingerprint: c.fingerprint(), TargetRegion: c.TargetRegion, DesiredRevision: revision, Scope: "binding:search"}
}
func signGrant(t *testing.T, private ed25519.PrivateKey, claims SearchGrantClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = "test-key"
	token.Header["typ"] = searchGrantType
	raw, err := token.SignedString(private)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestSignedSearchGrantScopeAndControlRevision(t *testing.T) {
	g, _, _, c := servingFixture(t)
	private, jwks := grantKeys(t)
	verifier, err := parseSearchGrantKeys(jwks)
	if err != nil {
		t.Fatal(err)
	}
	claims := grantClaims(c, 1, 60*time.Second)
	raw := signGrant(t, private, claims)
	if err = verifier.authorize(raw, c.BindingID, g.store); err != nil {
		t.Fatal("valid grant rejected", err)
	}
	handler, err := g.SignedHandler(c.BindingID, verifier)
	if err != nil {
		t.Fatal(err)
	}
	request := func(token string, want int) {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1/bindings/"+c.BindingID+"/search", strings.NewReader(`{"field":"title","term":"cedar"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("signed search got %d want %d", w.Code, want)
		}
		if want != 200 && strings.Contains(w.Body.String(), "cedar") {
			t.Fatal("authorization failure leaked rows")
		}
	}
	request(raw, 200)
	request(strings.Repeat("legacy-search-token-", 3), 401)
	for _, tc := range []struct {
		name   string
		change func(*SearchGrantClaims)
	}{
		{"wrong binding", func(c *SearchGrantClaims) { c.BindingID = "other" }},
		{"wrong region", func(c *SearchGrantClaims) { c.TargetRegion = "us-east-1" }},
		{"wrong mapping", func(c *SearchGrantClaims) { c.BindingFingerprint = strings.Repeat("0", 64) }},
		{"wrong revision", func(c *SearchGrantClaims) { c.DesiredRevision = 2 }},
		{"wrong scope", func(c *SearchGrantClaims) { c.Scope = "binding:write" }},
		{"wrong issuer", func(c *SearchGrantClaims) { c.Issuer = "other" }},
		{"wrong audience", func(c *SearchGrantClaims) { c.Audience = jwt.ClaimStrings{"other"} }},
		{"extra audience", func(c *SearchGrantClaims) { c.Audience = jwt.ClaimStrings{searchGrantAudience, "other"} }},
		{"expired", func(c *SearchGrantClaims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Second)) }},
		{"long grant", func(c *SearchGrantClaims) { c.ExpiresAt = jwt.NewNumericDate(c.IssuedAt.Add(61 * time.Second)) }},
		{"future issuance", func(c *SearchGrantClaims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute)) }},
		{"missing issuance", func(c *SearchGrantClaims) { c.IssuedAt = nil }},
		{"missing expiry", func(c *SearchGrantClaims) { c.ExpiresAt = nil }},
		{"wrong version", func(c *SearchGrantClaims) { c.ProtocolVersion = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := claims
			tc.change(&changed)
			request(signGrant(t, private, changed), 401)
		})
	}
	a := approvedTest(c)
	a.Control.DesiredRevision = 2
	a.Control.DesiredState = "paused"
	if err = g.store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	request(raw, 401)
	a.Control.DesiredRevision = 3
	a.Control.DesiredState = "running"
	if err = g.store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	request(raw, 401)
	// A new grant is authorized independently of serving readiness. Old epoch
	// resurrection remains forbidden by the existing managed execution protocol.
	if err = verifier.authorize(signGrant(t, private, grantClaims(c, 3, time.Minute)), c.BindingID, g.store); err != nil {
		t.Fatal(err)
	}
}
func TestSignedSearchRejectsExpiredGrantAfterRemoteRead(t *testing.T) {
	g, _, state, c := servingFixture(t)
	private, jwks := grantKeys(t)
	verifier, _ := parseSearchGrantKeys(jwks)
	handler, _ := g.SignedHandler(c.BindingID, verifier)
	// Cross a real NumericDate boundary inside the target call, without changing
	// global library clocks or process state used by other tests.
	claims := grantClaims(c, 1, 2*time.Second)
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(1500 * time.Millisecond))
	g.mu.Lock()
	g.current.target = searchFunc(func(context.Context, string, string, string) ([]map[string]any, int64, error) {
		time.Sleep(time.Until(claims.ExpiresAt.Time) + 10*time.Millisecond)
		return []map[string]any{document("1", state.Rows["1"])}, 1, nil
	})
	g.mu.Unlock()
	r := httptest.NewRequest("POST", "/v1/bindings/test/search", strings.NewReader(`{"field":"title","term":"cedar"}`))
	r.Header.Set("Authorization", "Bearer "+signGrant(t, private, claims))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 || strings.Contains(w.Body.String(), "cedar") {
		t.Fatal("expired in-flight grant exposed data")
	}
}
func TestSearchGrantRejectsUntrustedKeyMaterialAndAlgorithms(t *testing.T) {
	g, _, _, c := servingFixture(t)
	private, jwks := grantKeys(t)
	verifier, _ := parseSearchGrantKeys(jwks)
	claims := grantClaims(c, 1, time.Minute)
	for _, method := range []jwt.SigningMethod{jwt.SigningMethodHS256, jwt.SigningMethodNone} {
		token := jwt.NewWithClaims(method, claims)
		token.Header["kid"] = "test-key"
		token.Header["typ"] = searchGrantType
		var key any = []byte("synthetic-hmac")
		if method == jwt.SigningMethodNone {
			key = jwt.UnsafeAllowNoneSignatureType
		}
		raw, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		if verifier.authorize(raw, c.BindingID, g.store) == nil {
			t.Fatal("algorithm confusion accepted")
		}
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = "unknown"
	token.Header["typ"] = searchGrantType
	raw, _ := token.SignedString(private)
	if verifier.authorize(raw, c.BindingID, g.store) == nil {
		t.Fatal("unknown key accepted")
	}
	token.Header["kid"] = "test-key"
	token.Header["typ"] = "JWT"
	wrongPurpose, _ := token.SignedString(private)
	if verifier.authorize(wrongPurpose, c.BindingID, g.store) == nil {
		t.Fatal("generic JWT purpose accepted for search")
	}
	for _, raw := range []string{`{"keys":[]}`, `{"keys":[{"kid":"test-key","d":"private"}]}`, string(jwks) + `{}`, strings.Repeat("x", 8193)} {
		if _, err := parseSearchGrantKeys([]byte(raw)); err == nil {
			t.Fatal("invalid JWKS accepted")
		}
	}
	file := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(file, jwks, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSearchGrantVerifier(file); err != nil {
		t.Fatal(err)
	}
}
func TestCentralIssuedSearchGrantInterop(t *testing.T) {
	path := os.Getenv("CDC_TEST_SEARCH_GRANT_FIXTURE")
	if path == "" {
		t.Skip("set an external central-issued fixture path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Approved Approved        `json:"approved"`
		JWKS     json.RawMessage `json:"jwks"`
		Token    string          `json:"token"`
	}
	if json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("invalid central fixture")
	}
	g, _, _, c := servingFixtureFor(t, fixture.Approved.Binding)
	if err = g.store.AcceptControl(fixture.Approved, c.BindingID); err != nil {
		t.Fatal(err)
	}
	verifier, err := parseSearchGrantKeys(fixture.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := g.SignedHandler(c.BindingID, verifier)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/bindings/"+c.BindingID+"/search", strings.NewReader(`{"field":"title","term":"cedar"}`))
	request.Header.Set("Authorization", "Bearer "+fixture.Token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("actual central API grant rejected by worker HTTP handler: %d", response.Code)
	}

	// Never print the bearer. It is an ephemeral synthetic fixture, not a
	// production credential and not a maintained dependency on a private checkout.
	t.Log(fmt.Sprintf("central-issued grant verified locally for revision %d", fixture.Approved.Control.DesiredRevision))
}

func TestSearchGrantRotationAndWrongSignatures(t *testing.T) {
	g, _, _, c := servingFixture(t)
	oldKey, oldJWKS := grantKeys(t)
	newKey, newJWKS := grantKeys(t)
	var oldSet, newSet map[string][]map[string]string
	_ = json.Unmarshal(oldJWKS, &oldSet)
	_ = json.Unmarshal(newJWKS, &newSet)
	newSet["keys"][0]["kid"] = "next-key"
	overlap, _ := json.Marshal(map[string]any{"keys": []any{oldSet["keys"][0], newSet["keys"][0]}})
	verifier, err := parseSearchGrantKeys(overlap)
	if err != nil {
		t.Fatal(err)
	}
	claims := grantClaims(c, 1, time.Minute)
	oldGrant := signGrant(t, oldKey, claims)
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = "next-key"
	token.Header["typ"] = searchGrantType
	newGrant, _ := token.SignedString(newKey)
	if verifier.authorize(oldGrant, c.BindingID, g.store) != nil || verifier.authorize(newGrant, c.BindingID, g.store) != nil {
		t.Fatal("overlapping rotation keys rejected")
	}
	nextOnly, _ := json.Marshal(newSet)
	nextVerifier, err := parseSearchGrantKeys(nextOnly)
	if err != nil {
		t.Fatal(err)
	}
	if nextVerifier.authorize(oldGrant, c.BindingID, g.store) == nil || nextVerifier.authorize(newGrant, c.BindingID, g.store) != nil {
		t.Fatal("retired signing key still accepted")
	}
	forged := signGrant(t, newKey, claims)
	if verifier.authorize(forged, c.BindingID, g.store) == nil {
		t.Fatal("forged signature accepted under old key ID")
	}
	token.Header["jku"] = "https://untrusted.invalid/keys"
	injected, _ := token.SignedString(newKey)
	if verifier.authorize(injected, c.BindingID, g.store) == nil {
		t.Fatal("caller key URL accepted")
	}
	duplicate, _ := json.Marshal(map[string]any{"keys": []any{oldSet["keys"][0], oldSet["keys"][0]}})
	if _, err = parseSearchGrantKeys(duplicate); err == nil {
		t.Fatal("duplicate key IDs accepted")
	}
}

func TestSearchGrantCannotOutliveCurrentExecutionAuthority(t *testing.T) {
	g, _, _, c := servingFixture(t)
	private, jwks := grantKeys(t)
	verifier, _ := parseSearchGrantKeys(jwks)
	approved := approvedTest(c)
	approved.Control.DesiredRevision = 2
	approved.Control.AuthorityExpiresAt = time.Now().Add(20 * time.Second)
	if err := g.store.AcceptControl(approved, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if verifier.authorize(signGrant(t, private, grantClaims(c, 2, time.Minute)), c.BindingID, g.store) == nil {
		t.Fatal("grant outlived current authority")
	}
	if err := verifier.authorize(signGrant(t, private, grantClaims(c, 2, 5*time.Second)), c.BindingID, g.store); err != nil {
		t.Fatal("bounded grant rejected", err)
	}
}
