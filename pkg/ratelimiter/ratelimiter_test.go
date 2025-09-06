package ratelimiter

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfsv1 "github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/apis/zfs/v1"
)

func TestNewRateLimitedClient(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rateLimit := 10.0
	burst := 15

	rateLimitedClient := NewRateLimitedClient(baseClient, rateLimit, burst)

	if rateLimitedClient == nil {
		t.Fatal("Expected rate limited client to be created, got nil")
	}

	if rateLimitedClient.Client != baseClient {
		t.Error("Expected base client to be set correctly")
	}

	if rateLimitedClient.limiter == nil {
		t.Error("Expected limiter to be initialized")
	}
}

func TestRateLimitedClient_RateLimiting(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Set a very low rate limit for testing
	rateLimit := 2.0 // 2 requests per second
	burst := 1       // Only 1 request in burst

	rateLimitedClient := NewRateLimitedClient(baseClient, rateLimit, burst)

	// Create test objects
	zfsVolumeList := &zfsv1.ZFSVolumeList{}

	ctx := context.Background()
	startTime := time.Now()

	// Make multiple requests quickly
	numRequests := 3
	for i := 0; i < numRequests; i++ {
		err := rateLimitedClient.List(ctx, zfsVolumeList)
		if err != nil {
			t.Errorf("Request %d failed: %v", i+1, err)
		}
	}

	elapsed := time.Since(startTime)

	// With rate limiting, 3 requests at 2 req/sec should take at least 1 second
	// (first request immediate, second after 0.5s, third after 1s)
	expectedMinDuration := time.Millisecond * 900 // Allow some tolerance
	if elapsed < expectedMinDuration {
		t.Errorf("Rate limiting not working properly. Expected at least %v, got %v", expectedMinDuration, elapsed)
	}

	// But it shouldn't take too long either (max ~1.5s with tolerance)
	expectedMaxDuration := time.Second * 2
	if elapsed > expectedMaxDuration {
		t.Errorf("Rate limiting too aggressive. Expected at most %v, got %v", expectedMaxDuration, elapsed)
	}
}

func TestRateLimitedClient_ContextCancellation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Set a very low rate limit to force waiting
	rateLimit := 0.1 // 0.1 requests per second (10 second intervals)
	burst := 1

	rateLimitedClient := NewRateLimitedClient(baseClient, rateLimit, burst)

	zfsVolumeList := &zfsv1.ZFSVolumeList{}

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	// First request should succeed (uses burst)
	err := rateLimitedClient.List(ctx, zfsVolumeList)
	if err != nil {
		t.Errorf("First request should succeed, got: %v", err)
	}

	// Second request should fail due to context cancellation
	err = rateLimitedClient.List(ctx, zfsVolumeList)
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}

	// Check if it's a context-related error (could be DeadlineExceeded or other context error)
	if !strings.Contains(err.Error(), "context") && !strings.Contains(err.Error(), "deadline") {
		t.Errorf("Expected context-related error, got: %v", err)
	}
}

func TestRateLimitedClient_AllMethods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rateLimitedClient := NewRateLimitedClient(baseClient, 100.0, 100) // High limits for this test

	ctx := context.Background()

	// Test List
	zfsVolumeList := &zfsv1.ZFSVolumeList{}
	err := rateLimitedClient.List(ctx, zfsVolumeList)
	if err != nil {
		t.Errorf("List failed: %v", err)
	}

	// Test Get (will fail with NotFound, but that's expected)
	zfsVolume := &zfsv1.ZFSVolume{}
	err = rateLimitedClient.Get(ctx, client.ObjectKey{Name: "test", Namespace: "test"}, zfsVolume)
	// We expect this to fail with NotFound, but not with rate limiting errors
	if err != nil && err.Error() != `zfsvolumes.zfs.openebs.io "test" not found` {
		t.Errorf("Get failed with unexpected error: %v", err)
	}

	// Test Create
	testVolume := &zfsv1.ZFSVolume{}
	testVolume.SetName("test-volume")
	testVolume.SetNamespace("test-namespace")
	err = rateLimitedClient.Create(ctx, testVolume)
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}

	// Test Update (should work now that object exists)
	err = rateLimitedClient.Update(ctx, testVolume)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	// Test Delete
	err = rateLimitedClient.Delete(ctx, testVolume)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
}

func TestControllerRateLimiter(t *testing.T) {
	baseDelay := time.Millisecond * 100
	maxDelay := time.Second * 5

	rateLimiter := ControllerRateLimiter(baseDelay, maxDelay)

	if rateLimiter == nil {
		t.Fatal("Expected rate limiter to be created, got nil")
	}

	// Test that it returns delays for failures
	item := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-item", Namespace: "default"}}

	// First failure should have base delay
	delay := rateLimiter.When(item)
	if delay < baseDelay || delay > baseDelay*2 {
		t.Errorf("Expected delay around %v, got %v", baseDelay, delay)
	}

	// Add more failures to test exponential backoff
	rateLimiter.NumRequeues(item)
	delay2 := rateLimiter.When(item)
	if delay2 <= delay {
		t.Errorf("Expected exponential backoff, but delay2 (%v) <= delay1 (%v)", delay2, delay)
	}

	// Test forget
	rateLimiter.Forget(item)
	delay3 := rateLimiter.When(item)
	if delay3 >= delay2 {
		t.Errorf("Expected reset after forget, but delay3 (%v) >= delay2 (%v)", delay3, delay2)
	}
}

func TestRateLimitedClient_ConcurrentRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = zfsv1.AddToScheme(scheme)

	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Moderate rate limit for concurrent testing
	rateLimit := 5.0 // 5 requests per second
	burst := 2       // 2 requests in burst

	rateLimitedClient := NewRateLimitedClient(baseClient, rateLimit, burst)

	ctx := context.Background()
	numGoroutines := 5
	requestsPerGoroutine := 3

	// Channel to collect results
	results := make(chan error, numGoroutines*requestsPerGoroutine)

	startTime := time.Now()

	// Launch concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < requestsPerGoroutine; j++ {
				zfsVolumeList := &zfsv1.ZFSVolumeList{}
				err := rateLimitedClient.List(ctx, zfsVolumeList)
				results <- err
			}
		}(i)
	}

	// Collect all results
	var errors []error
	for i := 0; i < numGoroutines*requestsPerGoroutine; i++ {
		if err := <-results; err != nil {
			errors = append(errors, err)
		}
	}

	elapsed := time.Since(startTime)

	// Check that no errors occurred
	if len(errors) > 0 {
		t.Errorf("Expected no errors, got %d errors: %v", len(errors), errors)
	}

	// With 15 total requests at 5 req/sec with burst of 2:
	// - First 2 requests are immediate (burst)
	// - Remaining 13 requests need to wait for rate limiting
	// - Should take at least (13-1)/5 = 2.4 seconds
	expectedMinDuration := time.Second * 2
	if elapsed < expectedMinDuration {
		t.Errorf("Concurrent rate limiting not working properly. Expected at least %v, got %v", expectedMinDuration, elapsed)
	}

	// But shouldn't take too long either
	expectedMaxDuration := time.Second * 5
	if elapsed > expectedMaxDuration {
		t.Errorf("Rate limiting too aggressive for concurrent requests. Expected at most %v, got %v", expectedMaxDuration, elapsed)
	}
}
