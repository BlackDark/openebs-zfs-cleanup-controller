package ratelimiter

import (
	"context"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// RateLimitedClient wraps a Kubernetes client with rate limiting capabilities
type RateLimitedClient struct {
	client.Client
	limiter *rate.Limiter
}

// NewRateLimitedClient creates a new rate-limited client wrapper
func NewRateLimitedClient(client client.Client, rateLimit float64, burst int) *RateLimitedClient {
	limiter := rate.NewLimiter(rate.Limit(rateLimit), burst)
	return &RateLimitedClient{
		Client:  client,
		limiter: limiter,
	}
}

// Get wraps the client Get method with rate limiting
func (r *RateLimitedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.Get(ctx, key, obj, opts...)
}

// List wraps the client List method with rate limiting
func (r *RateLimitedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.List(ctx, list, opts...)
}

// Create wraps the client Create method with rate limiting
func (r *RateLimitedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.Create(ctx, obj, opts...)
}

// Delete wraps the client Delete method with rate limiting
func (r *RateLimitedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.Delete(ctx, obj, opts...)
}

// Update wraps the client Update method with rate limiting
func (r *RateLimitedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.Update(ctx, obj, opts...)
}

// Patch wraps the client Patch method with rate limiting
func (r *RateLimitedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.Patch(ctx, obj, patch, opts...)
}

// DeleteAllOf wraps the client DeleteAllOf method with rate limiting
func (r *RateLimitedClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.Client.DeleteAllOf(ctx, obj, opts...)
}

// SubResource wraps the client SubResource method
func (r *RateLimitedClient) SubResource(subResource string) client.SubResourceClient {
	return &RateLimitedSubResourceClient{
		SubResourceClient: r.Client.SubResource(subResource),
		limiter:           r.limiter,
	}
}

// RateLimitedSubResourceClient wraps a SubResourceClient with rate limiting
type RateLimitedSubResourceClient struct {
	client.SubResourceClient
	limiter *rate.Limiter
}

// Get wraps the SubResourceClient Get method with rate limiting
func (r *RateLimitedSubResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.SubResourceClient.Get(ctx, obj, subResource, opts...)
}

// Create wraps the SubResourceClient Create method with rate limiting
func (r *RateLimitedSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.SubResourceClient.Create(ctx, obj, subResource, opts...)
}

// Update wraps the SubResourceClient Update method with rate limiting
func (r *RateLimitedSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.SubResourceClient.Update(ctx, obj, opts...)
}

// Patch wraps the SubResourceClient Patch method with rate limiting
func (r *RateLimitedSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.SubResourceClient.Patch(ctx, obj, patch, opts...)
}

// ControllerRateLimiter creates a workqueue rate limiter for controller-runtime
func ControllerRateLimiter(baseDelay time.Duration, maxDelay time.Duration) workqueue.TypedRateLimiter[reconcile.Request] {
	return workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](baseDelay, maxDelay),
		// 10 qps, 100 bucket size for overall rate limiting
		&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}
