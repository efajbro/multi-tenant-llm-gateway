// Package k8s provides a bounded worker pool for Kubernetes API interactions.
//
// Key design decisions:
//   - Single shared clientset (HTTP/2 connection reuse) across all workers
//   - Semaphore via buffered chan struct{} for backpressure with no mutex contention
//   - Deterministic job names (SHA256 of tenant+model+prompt, 5s time bucket) for idempotency
//   - Exponential backoff retry for transient K8s API errors (5xx, network resets)
//   - context.Context propagation: client disconnect cancels the job submission
package k8s

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	maxRetries        = 3
	retryBaseDelay    = 100 * time.Millisecond
	timeBucketSeconds = 5
)

// JobRequest carries everything needed to construct and submit a Kueue Job.
type JobRequest struct {
	TenantID  string
	Model     string
	Prompt    string
	Namespace string
	Queue     string
	NodeID    string // optional: pin to specific node from scheduler
}

// JobResult is returned by Submit on success.
type JobResult struct {
	JobName string
	Created bool // false = AlreadyExists (idempotent retry)
}

// Pool is a bounded, concurrent Kubernetes API worker pool.
type Pool struct {
	client    kubernetes.Interface
	semaphore chan struct{}
}

// NewPool creates a Pool backed by in-cluster config (falls back to kubeconfig).
func NewPool(concurrency int) (*Pool, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("building k8s config: %w", err)
		}
	}
	cfg.QPS = 50
	cfg.Burst = 100
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating clientset: %w", err)
	}
	return &Pool{client: client, semaphore: make(chan struct{}, concurrency)}, nil
}

// NewPoolWithClient creates a Pool with a pre-built client (for testing).
func NewPoolWithClient(client kubernetes.Interface, concurrency int) *Pool {
	return &Pool{client: client, semaphore: make(chan struct{}, concurrency)}
}

// Submit acquires a pool slot, builds the Job manifest, and creates it in Kubernetes.
// It blocks until a pool slot is available or ctx is cancelled.
func (p *Pool) Submit(ctx context.Context, req JobRequest) (JobResult, error) {
	select {
	case p.semaphore <- struct{}{}:
		defer func() { <-p.semaphore }()
	case <-ctx.Done():
		return JobResult{}, fmt.Errorf("acquiring pool slot: %w", ctx.Err())
	}

	jobName := deterministicJobName(req)
	job := buildJob(req, jobName)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryDelay(attempt)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return JobResult{}, fmt.Errorf("retry interrupted: %w", ctx.Err())
			}
		}
		_, err := p.client.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{})
		if err == nil {
			return JobResult{JobName: jobName, Created: true}, nil
		}
		if k8serrors.IsAlreadyExists(err) {
			return JobResult{JobName: jobName, Created: false}, nil
		}
		if isNonRetryable(err) {
			return JobResult{}, fmt.Errorf("k8s job create (non-retryable): %w", err)
		}
		lastErr = err
	}
	return JobResult{}, fmt.Errorf("k8s job create after %d retries: %w", maxRetries, lastErr)
}

// Utilization returns the fraction of pool slots currently occupied (0.0–1.0).
func (p *Pool) Utilization() float64 {
	if cap(p.semaphore) == 0 { return 0 }
	return float64(len(p.semaphore)) / float64(cap(p.semaphore))
}

// deterministicJobName produces a stable name for a given (tenant, model, prompt, 5s bucket).
func deterministicJobName(req JobRequest) string {
	bucket := time.Now().Unix() / timeBucketSeconds
	promptHash := sha256.Sum256([]byte(req.Prompt))
	raw := fmt.Sprintf("%s:%s:%x:%d", req.TenantID, req.Model, promptHash[:8], bucket)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("llm-%x", h[:8])
}

func buildJob(req JobRequest, jobName string) *batchv1.Job {
	ns := req.Namespace
	if ns == "" { ns = "default" }
	queue := req.Queue
	if queue == "" { queue = "gpu-queue" }

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName, Namespace: ns,
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": queue,
				"ai-gateway/tenant":          req.TenantID,
				"ai-gateway/model":           req.Model,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "llm-runner",
						Image: "curlimages/curl:latest",
						Command: []string{"sh", "-c", fmt.Sprintf(
							`curl -s -X POST http://ollama-service:11434/api/generate `+
							`-H 'Content-Type: application/json' `+
							`-d '{"model":"%s","prompt":"%s","stream":false}'`,
							req.Model, sanitize(req.Prompt))},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					}},
				},
			},
		},
	}
	if req.NodeID != "" {
		job.Spec.Template.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": req.NodeID}
	}
	return job
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && i < 4096; i++ {
		c := s[i]
		// 34='"', 92=backslash, 10=LF, 13=CR
		if c == 34 || c == 92 || c == 10 || c == 13 {
			out = append(out, 92, c)
		} else {
			out = append(out, c)
		}
	}
	return string(out)
}

func isNonRetryable(err error) bool {
	return k8serrors.IsBadRequest(err) || k8serrors.IsForbidden(err) ||
		k8serrors.IsNotFound(err) || k8serrors.IsInvalid(err)
}

func retryDelay(attempt int) time.Duration {
	base := retryBaseDelay * (1 << uint(attempt))
	return base + time.Duration(rand.Int63n(int64(base/5)))
}

func int32Ptr(i int32) *int32 { return &i }
