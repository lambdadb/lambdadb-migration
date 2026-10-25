package postgrescdc

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const searchGrantIssuer = "lambdadb-cdc-management"
const searchGrantAudience = "lambdadb-cdc-search"
const searchGrantType = "lambdadb-cdc-search+jwt"
const maxSearchGrantLifetime = 60 * time.Second

type SearchGrantClaims struct {
	jwt.RegisteredClaims
	ProtocolVersion    int    `json:"protocol_version"`
	BindingID          string `json:"binding_id"`
	BindingFingerprint string `json:"binding_fingerprint"`
	TargetRegion       string `json:"target_region"`
	DesiredRevision    uint64 `json:"desired_revision"`
	Scope              string `json:"scope"`
}
type SearchGrantVerifier struct{ keys map[string]ed25519.PublicKey }

// LoadSearchGrantVerifier loads a bounded local JWKS once. Verification never
// fetches a key URL supplied by a caller or depends on central availability.
func LoadSearchGrantVerifier(path string) (*SearchGrantVerifier, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("search verification keys unavailable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(raw) > 8192 {
		return nil, errors.New("invalid search verification key file")
	}
	return parseSearchGrantKeys(raw)
}
func parseSearchGrantKeys(raw []byte) (*SearchGrantVerifier, error) {
	var jwks struct {
		Keys []struct {
			Kty string `json:"kty"`
			Crv string `json:"crv"`
			Kid string `json:"kid"`
			X   string `json:"x"`
			Alg string `json:"alg"`
			Use string `json:"use"`
		} `json:"keys"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var extra any
	if d.Decode(&jwks) != nil || d.Decode(&extra) != io.EOF || len(jwks.Keys) == 0 || len(jwks.Keys) > 8 {
		return nil, errors.New("invalid search verification keys")
	}
	verifier := &SearchGrantVerifier{keys: map[string]ed25519.PublicKey{}}
	for _, key := range jwks.Keys {
		public, err := base64.RawURLEncoding.Strict().DecodeString(key.X)
		if err != nil || len(public) != ed25519.PublicKeySize || key.Kty != "OKP" || key.Crv != "Ed25519" || key.Alg != "EdDSA" || key.Use != "sig" || !controlID.MatchString(key.Kid) || verifier.keys[key.Kid] != nil {
			return nil, errors.New("invalid search verification keys")
		}
		verifier.keys[key.Kid] = ed25519.PublicKey(public)
	}
	return verifier, nil
}
func (v *SearchGrantVerifier) verify(raw, bindingID string) (*SearchGrantClaims, error) {
	if len(raw) > 2048 {
		return nil, errors.New("invalid search grant")
	}
	claims := &SearchGrantClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok || token.Header["typ"] != searchGrantType || len(token.Header) != 3 || v.keys[kid] == nil {
			return nil, errors.New("invalid search grant header")
		}
		return v.keys[kid], nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(searchGrantIssuer), jwt.WithAudience(searchGrantAudience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithStrictDecoding())
	if err != nil || !token.Valid || claims.ProtocolVersion != 1 || claims.BindingID != bindingID || claims.Scope != "binding:search" || claims.DesiredRevision == 0 || claims.Subject == "" || claims.ID == "" || claims.IssuedAt == nil || claims.NotBefore == nil || claims.ExpiresAt == nil || !claims.ExpiresAt.After(claims.IssuedAt.Time) || claims.ExpiresAt.Sub(claims.IssuedAt.Time) > maxSearchGrantLifetime || !claims.NotBefore.Equal(claims.IssuedAt.Time) || len(claims.Audience) != 1 {
		return nil, errors.New("invalid search grant")
	}
	return claims, nil
}

// Authorize binds signed claims to the current cached control command. It is
// repeated after query validation so a grant racing control changes cannot pass.
func (v *SearchGrantVerifier) authorize(raw, bindingID string, store *Store) error {
	claims, err := v.verify(raw, bindingID)
	if err != nil {
		return err
	}
	approved, err := store.Approved()
	if err != nil || approved == nil || approved.Binding.BindingID != bindingID || claims.BindingFingerprint != approved.Binding.fingerprint() || claims.TargetRegion != approved.Binding.TargetRegion || claims.DesiredRevision != approved.Control.DesiredRevision || approved.Control.DesiredState != "running" || !time.Now().Before(approved.Control.AuthorityExpiresAt) || claims.ExpiresAt.After(approved.Control.AuthorityExpiresAt) {
		return errors.New("search grant authority changed")
	}
	return nil
}
