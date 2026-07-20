package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func makeTestValidator(t *testing.T) (*Validator, *rsa.PrivateKey) {
	t.Helper()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { t.Fatalf("keygen: %v", err) }
	return &Validator{publicKey: &privKey.PublicKey}, privKey
}

func signToken(t *testing.T, privKey *rsa.PrivateKey, claims *TenantClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	s, err := tok.SignedString(privKey)
	if err != nil { t.Fatalf("sign: %v", err) }
	return s
}

func TestValidToken(t *testing.T) {
	v, priv := makeTestValidator(t)
	claims := &TenantClaims{
		TenantID: "tenant-abc",
		Tier: TierPremium,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "tenant-abc",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	got, err := v.Validate(signToken(t, priv, claims))
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if got.TenantID != "tenant-abc" { t.Errorf("want tenant-abc got %q", got.TenantID) }
}

func TestExpiredToken(t *testing.T) {
	v, priv := makeTestValidator(t)
	claims := &TenantClaims{
		TenantID: "t",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "t",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		},
	}
	_, err := v.Validate(signToken(t, priv, claims))
	if err == nil { t.Fatal("expected error for expired token") }
}

func TestWrongAlgorithm(t *testing.T) {
	v, _ := makeTestValidator(t)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "attacker",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, _ := tok.SignedString([]byte("secret"))
	_, err := v.Validate(signed)
	if err == nil { t.Fatal("expected error for HMAC token") }
}

func TestMissingSubClaim(t *testing.T) {
	v, priv := makeTestValidator(t)
	claims := &TenantClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	_, err := v.Validate(signToken(t, priv, claims))
	if err == nil || !strings.Contains(err.Error(), "sub") {
		t.Fatalf("expected sub error, got: %v", err)
	}
}

func TestKeyRotation(t *testing.T) {
	v, oldPriv := makeTestValidator(t)
	claims := &TenantClaims{
		TenantID: "xyz",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "xyz",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	oldToken := signToken(t, oldPriv, claims)

	newPriv, _ := rsa.GenerateKey(rand.Reader, 2048)
	v.RotateKey(&newPriv.PublicKey, "kid-2")

	// old token should fail
	if _, err := v.Validate(oldToken); err == nil {
		t.Fatal("old token should fail after rotation")
	}
	// new token should pass
	newToken := signToken(t, newPriv, claims)
	if got, err := v.Validate(newToken); err != nil || got.TenantID != "xyz" {
		t.Fatalf("new token failed: %v", err)
	}
}
