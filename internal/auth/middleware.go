// Package auth provides JWT RS256 authentication middleware.
// Tenant identity is extracted from validated tokens and injected into
// request context — downstream code never reads raw headers.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Tier represents the tenant service tier driving quota enforcement.
type Tier string

const (
	TierPremium  Tier = "premium"
	TierStandard Tier = "standard"
	TierFree     Tier = "free"
)

// TenantClaims is the JWT payload. All rate-limit / quota data lives here,
// so the hot path never needs a database lookup.
type TenantClaims struct {
	TenantID         string   `json:"sub"`
	Tier             Tier     `json:"tier"`
	AllowedModels    []string `json:"models"`
	RatePerSecond    float64  `json:"rps"`
	MaxContextTokens int      `json:"max_ctx"`
	jwt.RegisteredClaims
}

type contextKey string
const claimsKey contextKey = "tenant_claims"

// WithClaims injects validated TenantClaims into the context.
func WithClaims(ctx context.Context, c *TenantClaims) context.Context {
	return context.WithValue(ctx, claimsKey, c)
}

// ClaimsFromContext retrieves TenantClaims from context (nil, false if unauthenticated).
func ClaimsFromContext(ctx context.Context) (*TenantClaims, bool) {
	c, ok := ctx.Value(claimsKey).(*TenantClaims)
	return c, ok
}

// Validator holds the RSA public key. Safe for concurrent use.
type Validator struct {
	mu        sync.RWMutex
	publicKey *rsa.PublicKey
	keyID     string
}

// NewValidatorFromPEM creates a Validator from a PEM-encoded RSA public key.
func NewValidatorFromPEM(pemBytes []byte) (*Validator, error) {
	key, err := jwt.ParseRSAPublicKeyFromPEM(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing RSA public key: %w", err)
	}
	return &Validator{publicKey: key}, nil
}

// RotateKey atomically replaces the public key (zero-downtime key rotation).
func (v *Validator) RotateKey(newKey *rsa.PublicKey, newKID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.publicKey = newKey
	v.keyID = newKID
}

// Validate parses and validates a JWT, returning TenantClaims on success.
func (v *Validator) Validate(tokenString string) (*TenantClaims, error) {
	v.mu.RLock()
	pub := v.publicKey
	v.mu.RUnlock()

	claims := &TenantClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return pub, nil
	}, jwt.WithExpirationRequired())
	if err != nil {
		return nil, fmt.Errorf("token validation: %w", err)
	}
	if !token.Valid {
		return nil, errors.New("token not valid")
	}
	if claims.TenantID == "" {
		return nil, errors.New("missing sub claim")
	}
	return claims, nil
}

// Middleware returns an HTTP middleware that validates Bearer JWTs.
// Unauthenticated requests receive 401 with a structured JSON error.
func Middleware(v *Validator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, "missing Authorization header")
				return
			}
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
				writeAuthError(w, http.StatusUnauthorized, "Authorization must be 'Bearer <token>'")
				return
			}
			claims, err := v.Validate(parts[1])
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, err.Error())
				return
			}
			ctx := WithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type apiError struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	Timestamp string `json:"timestamp"`
}

func writeAuthError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(apiError{
		Error:     msg,
		Code:      "UNAUTHENTICATED",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	_, _ = w.Write(body)
}
