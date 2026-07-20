// Command gateway is the production AI Gateway.
//
// Environment variables:
//   PORT              HTTP port (default: 8080)
//   LOG_MODE          "production" | "development" (default: production)
//   REDIS_ADDR        Redis address (default: redis:6379)
//   GLOBAL_RPS        Global requests/sec ceiling (default: 200)
//   K8S_WORKERS       K8s API worker pool size (default: 20)
//   OLLAMA_BASE_URL   Ollama service URL (default: http://ollama-service:11434)
//   JWT_PUBLIC_KEY_PEM  RSA public key PEM string (required unless DEV_MODE=true)
//   DEFAULT_NAMESPACE Kubernetes namespace for jobs (default: default)
//   DEV_MODE          "true" to load JWT key from fixtures/dev_public.pem
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"ai-gateway/internal/auth"
	"ai-gateway/internal/health"
	"ai-gateway/internal/k8s"
	"ai-gateway/internal/logging"
	"ai-gateway/internal/metrics"
	"ai-gateway/internal/ratelimit"
	"ai-gateway/internal/stream"

	"go.uber.org/zap"
)

type config struct {
	Port, LogMode, RedisAddr, OllamaBaseURL string
	JWTPubKeyPEM, DefaultNamespace          string
	GlobalRPS, K8sWorkers                   int
	DevMode                                 bool
}

func loadConfig() config {
	return config{
		Port:             getenv("PORT", "8080"),
		LogMode:          getenv("LOG_MODE", "production"),
		RedisAddr:        getenv("REDIS_ADDR", "redis:6379"),
		GlobalRPS:        envInt("GLOBAL_RPS", 200),
		K8sWorkers:       envInt("K8S_WORKERS", 20),
		OllamaBaseURL:    getenv("OLLAMA_BASE_URL", "http://ollama-service:11434"),
		JWTPubKeyPEM:     getenv("JWT_PUBLIC_KEY_PEM", ""),
		DefaultNamespace: getenv("DEFAULT_NAMESPACE", "default"),
		DevMode:          getenv("DEV_MODE", "false") == "true",
	}
}

func main() {
	cfg := loadConfig()

	if err := logging.Init(cfg.LogMode); err != nil {
		fmt.Fprintf(os.Stderr, "logger init: %v\n", err); os.Exit(1)
	}
	defer logging.Sync()
	log := logging.L()
	log.Info("AI Gateway starting", zap.String("port", cfg.Port))

	m := metrics.New()

	limiter, err := ratelimit.NewLimiter(cfg.RedisAddr, cfg.GlobalRPS)
	if err != nil { log.Fatal("Redis failed", zap.Error(err)) }
	defer limiter.Close()
	log.Info("Redis connected", zap.String("addr", cfg.RedisAddr))

	pool, err := k8s.NewPool(cfg.K8sWorkers)
	if err != nil { log.Fatal("K8s pool failed", zap.Error(err)) }
	log.Info("K8s worker pool ready", zap.Int("workers", cfg.K8sWorkers))

	var validator *auth.Validator
	if cfg.DevMode {
		log.Warn("DEV_MODE=true — JWT disabled, loading from fixtures/dev_public.pem")
		validator = loadDevValidator(log)
	} else {
		if cfg.JWTPubKeyPEM == "" {
			log.Fatal("JWT_PUBLIC_KEY_PEM required (or DEV_MODE=true)")
		}
		validator, err = auth.NewValidatorFromPEM([]byte(cfg.JWTPubKeyPEM))
		if err != nil { log.Fatal("JWT key parse failed", zap.Error(err)) }
		log.Info("JWT RS256 validator ready")
	}

	streamer := stream.NewStreamer(cfg.OllamaBaseURL)
	healthHandler := health.New()
	healthHandler.Register(health.NewRedisChecker("redis", limiter.Ping))

	mux := http.NewServeMux()
	authMW := auth.Middleware(validator)
	mux.Handle("/v1/completions", authMW(http.HandlerFunc(makeCompletions(cfg, m, limiter, pool, streamer, log))))
	mux.Handle("/v1/chat/completions", authMW(http.HandlerFunc(makeChat(cfg, m, limiter, pool, streamer, log))))
	mux.HandleFunc("/v1/models", modelsHandler)
	mux.HandleFunc("/healthz", healthHandler.Liveness)
	mux.HandleFunc("/readyz", healthHandler.Readiness)
	mux.Handle("/metrics", m.Handler())

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C { m.WorkerPoolUtilization.Set(pool.Utilization()) }
	}()

	srv := &http.Server{
		Addr: ":" + cfg.Port, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      6 * time.Minute,
		IdleTimeout:       120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Info("Listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	<-quit
	log.Info("Shutdown signal — draining (30s)...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil { log.Error("shutdown failed", zap.Error(err)) }
	log.Info("Gateway shut down cleanly")
}

func makeCompletions(cfg config, m *metrics.Metrics, limiter *ratelimit.Limiter, pool *k8s.Pool, streamer *stream.Streamer, log *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { writeErr(w, 405, "method_not_allowed", "POST required"); return }

		start := time.Now()
		claims, _ := auth.ClaimsFromContext(r.Context())
		tenantID, model, rps := extractTenant(claims)

		rl := limiter.Allow(r.Context(), tenantID, model, rps)
		if !rl.Allowed {
			m.RateLimitTotal.WithLabelValues(tenantID, rl.LimitType).Inc()
			log.Warn("rate limited", zap.String("tenant", tenantID), zap.String("layer", rl.LimitType))
			w.Header().Set("Retry-After", strconv.Itoa(rl.RetryAfterSecs))
			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(rl.Limit, 10))
			w.Header().Set("X-RateLimit-Remaining", "0")
			writeErr(w, 429, "rate_limit_exceeded", fmt.Sprintf("limit exceeded at layer: %s", rl.LimitType))
			return
		}

		var req struct {
			Model  string `json:"model"`
			Prompt string `json:"prompt"`
			Stream bool   `json:"stream"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			writeErr(w, 400, "invalid_request", err.Error()); return
		}
		if req.Model != "" { model = req.Model }
		if req.Prompt == "" { writeErr(w, 400, "invalid_request", "prompt required"); return }

		requestID := fmt.Sprintf("cmpl-%d", time.Now().UnixNano())
		reqLog := log.With(
			zap.String("request_id", requestID),
			zap.String("tenant", tenantID),
			zap.String("model", model),
		)

		jobResult, err := pool.Submit(r.Context(), k8s.JobRequest{
			TenantID: tenantID, Model: model, Prompt: req.Prompt,
			Namespace: cfg.DefaultNamespace, Queue: queueForTier(claims),
		})
		if err != nil {
			reqLog.Error("job submit failed", zap.Error(err))
			writeErr(w, 500, "job_submit_failed", err.Error())
			m.RequestsTotal.WithLabelValues(tenantID, model, "500").Inc()
			return
		}
		reqLog.Info("job submitted", zap.String("job", jobResult.JobName), zap.Bool("created", jobResult.Created))
		m.ActiveJobs.WithLabelValues(tenantID, model).Inc()
		defer m.ActiveJobs.WithLabelValues(tenantID, model).Dec()

		if req.Stream {
			if err := streamer.Stream(r.Context(), w, model, req.Prompt, requestID); err != nil && r.Context().Err() == nil {
				reqLog.Error("stream error", zap.Error(err))
			}
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id": requestID, "job_name": jobResult.JobName, "status": "queued",
			})
		}

		dur := time.Since(start).Seconds()
		m.RequestDuration.WithLabelValues(tenantID, model).Observe(dur)
		m.RequestsTotal.WithLabelValues(tenantID, model, "200").Inc()
		reqLog.Info("request complete", zap.Float64("duration_s", dur))
	}
}

func makeChat(cfg config, m *metrics.Metrics, limiter *ratelimit.Limiter, pool *k8s.Pool, streamer *stream.Streamer, log *zap.Logger) http.HandlerFunc {
	completions := makeCompletions(cfg, m, limiter, pool, streamer, log)
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Stream   bool   `json:"stream"`
			Messages []struct{ Role, Content string } `json:"messages"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			writeErr(w, 400, "invalid_request", err.Error()); return
		}
		var prompt string
		for _, msg := range req.Messages { prompt += fmt.Sprintf("[%s]: %s\n", msg.Role, msg.Content) }
		body, _ := json.Marshal(map[string]interface{}{"model": req.Model, "prompt": prompt, "stream": req.Stream})
		r2 := r.Clone(r.Context())
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
		completions(w, r2)
	}
}

func modelsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"object": "list",
		"data": []map[string]interface{}{
			{"id": "llama3:8b", "object": "model", "owned_by": "ai-gateway"},
			{"id": "llama3:70b", "object": "model", "owned_by": "ai-gateway"},
			{"id": "mistral:7b", "object": "model", "owned_by": "ai-gateway"},
		},
	})
}

func extractTenant(claims *auth.TenantClaims) (tenantID, model string, rps float64) {
	if claims == nil { return "anonymous", "llama3:8b", 1.0 }
	model = "llama3:8b"
	if len(claims.AllowedModels) > 0 { model = claims.AllowedModels[0] }
	return claims.TenantID, model, claims.RatePerSecond
}

func queueForTier(claims *auth.TenantClaims) string {
	if claims != nil && claims.Tier == auth.TierPremium { return "premium-queue" }
	return "gpu-queue"
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]string{"code": code, "message": msg},
	})
}

func getenv(k, def string) string { if v := os.Getenv(k); v != "" { return v }; return def }
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" { if n, err := strconv.Atoi(v); err == nil { return n } }
	return def
}

func loadDevValidator(log *zap.Logger) *auth.Validator {
	pemPath := getenv("DEV_JWT_PUBLIC_KEY_PATH", "fixtures/dev_public.pem")
	pemBytes, err := os.ReadFile(pemPath)
	if err != nil {
		log.Fatal("fixtures/dev_public.pem missing. Generate with:\n"+
			"  mkdir -p fixtures\n"+
			"  openssl genrsa -out fixtures/dev_private.pem 2048\n"+
			"  openssl rsa -in fixtures/dev_private.pem -pubout -out fixtures/dev_public.pem",
			zap.String("path", pemPath), zap.Error(err))
	}
	v, err := auth.NewValidatorFromPEM(pemBytes)
	if err != nil { log.Fatal("dev key parse failed", zap.Error(err)) }
	log.Info("Dev JWT validator loaded", zap.String("key", pemPath))
	return v
}
