// Package ratelimit implements a distributed sliding window rate limiter
// backed by Redis. Three independent layers are checked per request:
// global, per-tenant, and per-(tenant,model).
package ratelimit

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const globalKey = "ratelimit:global"

var memberCounter int64

// slidingWindowScript atomically records a request, prunes old entries,
// and returns the current count within the window.
// KEYS[1]=key ARGV[1]=now_ns ARGV[2]=window_start_ns ARGV[3]=window_secs ARGV[4]=unique_member
// We pass a unique member (now_ns + random suffix) so concurrent nanosecond calls
// don't overwrite each other in the sorted set.
var slidingWindowScript = redis.NewScript(`
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local win_start = tonumber(ARGV[2])
local win_ms    = tonumber(ARGV[3])
local member    = ARGV[4]
redis.call('ZADD', key, now, member)
redis.call('ZREMRANGEBYSCORE', key, '-inf', win_start)
redis.call('PEXPIRE', key, win_ms * 2)
return redis.call('ZCARD', key)
`)

// Limiter is the rate limiter. Safe for concurrent use.
type Limiter struct {
	rdb           *redis.Client
	globalLimit   int
	globalWindow  time.Duration
	defaultWindow time.Duration
}

// NewLimiter creates a Limiter connected to Redis at addr.
func NewLimiter(addr string, globalLimit int) (*Limiter, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     20,
		MinIdleConns: 5,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Limiter{
		rdb:           rdb,
		globalLimit:   globalLimit,
		globalWindow:  time.Second,
		defaultWindow: time.Second,
	}, nil
}

// Result carries the outcome of an Allow check.
type Result struct {
	Allowed        bool
	LimitType      string // "global" | "tenant" | "model"
	CurrentCount   int64
	Limit          int64
	RetryAfterSecs int
}

// Allow checks all three rate-limit layers. Fail-open on Redis errors.
func (l *Limiter) Allow(ctx context.Context, tenantID, model string, tenantRPS float64) Result {
	if r := l.check(ctx, globalKey, l.globalLimit, l.globalWindow, "global"); !r.Allowed {
		return r
	}
	tenantLimit := int(tenantRPS)
	if tenantLimit <= 0 {
		tenantLimit = 10
	}
	tenantKey := fmt.Sprintf("ratelimit:tenant:%s", tenantID)
	if r := l.check(ctx, tenantKey, tenantLimit, l.defaultWindow, "tenant"); !r.Allowed {
		return r
	}
	modelKey := fmt.Sprintf("ratelimit:model:%s:%s", tenantID, model)
	ml := modelLimit(model, tenantLimit)
	if r := l.check(ctx, modelKey, ml, l.defaultWindow, "model"); !r.Allowed {
		return r
	}
	return Result{Allowed: true}
}

func (l *Limiter) check(ctx context.Context, key string, limit int, window time.Duration, layer string) Result {
	now := time.Now().UnixMilli()
	winStart := now - window.Milliseconds()
	// Unique member = timestamp + random int32 to handle sub-nanosecond colissions
member := fmt.Sprintf("%d:%d", now, atomic.AddInt64(&memberCounter, 1))
res := slidingWindowScript.Run(ctx, l.rdb,
		[]string{key},
		now, winStart, int(window.Milliseconds()), member,
	)
	if res.Err() != nil {
		return Result{Allowed: true} // fail open
	}
	count, _ := res.Int64()
	if int(count) > limit {
		return Result{
			Allowed: false, LimitType: layer,
			CurrentCount: count, Limit: int64(limit),
			RetryAfterSecs: int(window.Seconds()),
		}
	}
	return Result{Allowed: true, CurrentCount: count, Limit: int64(limit), LimitType: layer}
}

func modelLimit(model string, tenantLimit int) int {
	switch model {
	case "llama3:70b", "mixtral:8x7b":
		if tenantLimit > 2 { return 2 }
	case "llama3:8b", "mistral:7b":
		if tenantLimit > 5 { return 5 }
	}
	return tenantLimit
}

// Ping checks Redis connectivity (used by readiness probe).
func (l *Limiter) Ping(ctx context.Context) error { return l.rdb.Ping(ctx).Err() }

// Close closes the Redis connection pool.
func (l *Limiter) Close() error { return l.rdb.Close() }
