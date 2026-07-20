package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T, globalLimit int) (*Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	l := &Limiter{
		rdb:           rdb,
		globalLimit:   globalLimit,
		globalWindow:  time.Second,
		defaultWindow: time.Second,
	}
	return l, mr
}

func TestAllowUnderLimit(t *testing.T) {
	l, _ := newTestLimiter(t, 1000)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if r := l.Allow(ctx, "t1", "llama3:8b", 10); !r.Allowed {
			t.Fatalf("req %d denied unexpectedly: layer=%s", i, r.LimitType)
		}
	}
}

func TestDenyOverTenantLimit(t *testing.T) {
	// Use a very short window (100ms) so we can wait for real expiry without making
	// the test slow. miniredis.FastForward affects EXPIRE/TTL but not ZRANGEBYSCORE
	// because the Lua script uses time.Now() (process clock) not Redis time.
	l, _ := newTestLimiter(t, 1000)
	l.defaultWindow = 100 * time.Millisecond
	ctx := context.Background()

	tenantRPS := float64(3)
	for i := 0; i < 3; i++ {
		l.Allow(ctx, "t2", "llama3:8b", tenantRPS)
	}
	if r := l.Allow(ctx, "t2", "llama3:8b", tenantRPS); r.Allowed {
		t.Fatal("4th request should be denied at tenant layer")
	} else if r.LimitType != "tenant" {
		t.Errorf("expected tenant layer, got %q", r.LimitType)
	}

	// Wait for the real window to expire
	time.Sleep(120 * time.Millisecond)

	if r := l.Allow(ctx, "t2", "llama3:8b", tenantRPS); !r.Allowed {
		t.Fatal("should be allowed after window expires")
	}
}

func TestModelLayerThrottle(t *testing.T) {
	l, _ := newTestLimiter(t, 1000)
	ctx := context.Background()
	// llama3:70b has a per-(tenant,model) limit of 2
	l.Allow(ctx, "t3", "llama3:70b", 100)
	l.Allow(ctx, "t3", "llama3:70b", 100)
	if r := l.Allow(ctx, "t3", "llama3:70b", 100); r.Allowed {
		t.Fatal("3rd 70B request should be blocked at model layer")
	} else if r.LimitType != "model" {
		t.Errorf("expected model layer, got %q", r.LimitType)
	}
}

func TestConcurrentNoRace(t *testing.T) {
	l, _ := newTestLimiter(t, 10000)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Allow(ctx, "tc", "llama3:8b", 50)
		}()
	}
	wg.Wait()
}
