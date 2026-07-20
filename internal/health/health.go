// Package health provides /healthz (liveness) and /readyz (readiness) handlers.
// Checkers are run concurrently with a 3-second timeout.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Checker is a named health dependency check.
type Checker interface {
	Name() string
	Check(ctx context.Context) error
}

// Handler holds registered Checkers and exposes probe endpoints.
type Handler struct {
	mu       sync.RWMutex
	checkers []Checker
}

// New creates a Handler with no registered checkers.
func New() *Handler { return &Handler{} }

// Register adds a Checker to the readiness probe.
func (h *Handler) Register(c Checker) {
	h.mu.Lock(); defer h.mu.Unlock()
	h.checkers = append(h.checkers, c)
}

// Liveness handles GET /healthz. Always returns 200.
func (h *Handler) Liveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

type checkResult struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Readiness handles GET /readyz. Returns 503 if any checker fails.
func (h *Handler) Readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	h.mu.RLock()
	checkers := make([]Checker, len(h.checkers))
	copy(checkers, h.checkers)
	h.mu.RUnlock()

	results := make([]checkResult, len(checkers))
	var wg sync.WaitGroup
	allOK := true
	var mu sync.Mutex

	for i, c := range checkers {
		wg.Add(1)
		go func(idx int, ch Checker) {
			defer wg.Done()
			cr := checkResult{Name: ch.Name(), Status: "ok"}
			if err := ch.Check(ctx); err != nil {
				cr.Status = "fail"; cr.Message = err.Error()
				mu.Lock(); allOK = false; mu.Unlock()
			}
			results[idx] = cr
		}(i, c)
	}
	wg.Wait()

	status, httpStatus := "ok", http.StatusOK
	if !allOK { status, httpStatus = "degraded", http.StatusServiceUnavailable }

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": status, "checks": results,
		"checked_at": time.Now().UTC().Format(time.RFC3339),
	})
}

// RedisChecker implements Checker for a Redis Ping.
type RedisChecker struct{ name string; pingFn func(context.Context) error }

// NewRedisChecker creates a Checker that pings Redis via the provided function.
func NewRedisChecker(name string, pingFn func(context.Context) error) *RedisChecker {
	return &RedisChecker{name: name, pingFn: pingFn}
}

func (r *RedisChecker) Name() string                        { return r.name }
func (r *RedisChecker) Check(ctx context.Context) error    { return r.pingFn(ctx) }
