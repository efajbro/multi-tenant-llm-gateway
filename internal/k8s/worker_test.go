package k8s

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestSubmitCreatesJob(t *testing.T) {
	client := fake.NewSimpleClientset()
	pool := NewPoolWithClient(client, 10)
	req := JobRequest{TenantID: "t1", Model: "llama3:8b", Prompt: "hello", Namespace: "default"}
	r, err := pool.Submit(context.Background(), req)
	if err != nil { t.Fatalf("submit failed: %v", err) }
	if !r.Created { t.Error("expected Created=true") }
	if r.JobName == "" { t.Error("expected non-empty JobName") }
}

func TestSubmitIdempotency(t *testing.T) {
	client := fake.NewSimpleClientset()
	pool := NewPoolWithClient(client, 10)
	req := JobRequest{TenantID: "t2", Model: "llama3:8b", Prompt: "same", Namespace: "default"}
	r1, err := pool.Submit(context.Background(), req)
	if err != nil { t.Fatalf("first submit: %v", err) }

	// Inject AlreadyExists for second attempt
	client.Fake.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		job := ca.GetObject().(*batchv1.Job)
		if job.Name == r1.JobName {
			return true, nil, k8serrors.NewAlreadyExists(
				schema.GroupResource{Group: "batch", Resource: "jobs"}, job.Name)
		}
		return false, nil, nil
	})

	r2, err := pool.Submit(context.Background(), req)
	if err != nil { t.Fatalf("idempotent submit: %v", err) }
	if r2.Created { t.Error("expected Created=false on retry") }
	if r1.JobName != r2.JobName { t.Errorf("job names differ: %q vs %q", r1.JobName, r2.JobName) }
}

func TestSubmitContextCancellation(t *testing.T) {
	client := fake.NewSimpleClientset()
	pool := NewPoolWithClient(client, 1)
	pool.semaphore <- struct{}{} // block the only slot

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := pool.Submit(ctx, JobRequest{TenantID: "t", Model: "m", Prompt: "p", Namespace: "default"})
	if err == nil { t.Fatal("expected error when pool is full and ctx times out") }
	<-pool.semaphore
}

func TestUtilization(t *testing.T) {
	pool := NewPoolWithClient(fake.NewSimpleClientset(), 10)
	if pool.Utilization() != 0 { t.Errorf("expected 0, got %f", pool.Utilization()) }
	for i := 0; i < 5; i++ { pool.semaphore <- struct{}{} }
	if pool.Utilization() != 0.5 { t.Errorf("expected 0.5, got %f", pool.Utilization()) }
	for i := 0; i < 5; i++ { <-pool.semaphore }
}
