package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type TokenBucket struct {
	tokens  int
	max     int
	mu      sync.Mutex
	lastRef time.Time
	rate    time.Duration
}

func NewTokenBucket(max int, rate time.Duration) *TokenBucket {
	return &TokenBucket{
		tokens:  max,
		max:     max,
		lastRef: time.Now(),
		rate:    rate,
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRef)
	tokensToAdd := int(elapsed / tb.rate)

	if tokensToAdd > 0 {
		tb.tokens += tokensToAdd
		if tb.tokens > tb.max {
			tb.tokens = tb.max
		}
		tb.lastRef = now
	}

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

var tenantLimiters = make(map[string]*TokenBucket)
var mapMutex sync.Mutex

func getTenantLimiter(tenantID string) *TokenBucket {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	limiter, exists := tenantLimiters[tenantID]
	if !exists {
		limiter = NewTokenBucket(2, time.Second/2)
		tenantLimiters[tenantID] = limiter
	}
	return limiter
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}

	limiter := getTenantLimiter(tenantID)
	if !limiter.Allow() {
		http.Error(w, "429 Too Many Requests - GPU Protected", http.StatusTooManyRequests)
		return
	}

	body, _ := io.ReadAll(r.Body)
	prompt := string(body)
	if prompt == "" {
		prompt = "Explain the concept of time."
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		http.Error(w, "Failed to load K8s config. Note: Gateway must run inside a pod.", http.StatusInternalServerError)
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		http.Error(w, "Failed to create K8s client", http.StatusInternalServerError)
		return
	}

	jobName := fmt.Sprintf("llm-job-%d", time.Now().UnixNano())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: "default",
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": "gpu-queue",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "ollama-client",
							Image: "curlimages/curl:latest",
							Command: []string{
								"sh", "-c",
								fmt.Sprintf(`curl -X POST http://ollama-service:11434/api/generate -d '{"model": "llama3", "prompt": "%s", "stream": false}'`, prompt),
							},
						},
					},
				},
			},
		},
	}

	_, err = clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit job: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Job %s accepted and submitted to Kueue for tenant %s.", jobName, tenantID)
}

func main() {
	http.HandleFunc("/v1/completions", handleRequest)
	fmt.Println("AI Gateway with Kueue Integration online. Listening on port 8080...")
	http.ListenAndServe(":8080", nil)
}