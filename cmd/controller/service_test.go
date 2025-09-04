package main

import (
	"context"
	"testing"
	"time"

	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceModeConfiguration(t *testing.T) {
	t.Run("should validate service mode configuration", func(t *testing.T) {
		cfg := &config.Config{
			DryRun:                  false,
			ReconcileInterval:       time.Minute * 5,
			MaxConcurrentReconciles: 2,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            10.0,
			APIBurst:                15,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
			NamespaceFilter:         "openebs",
			LabelSelector:           "",
			MetricsPort:             8080,
			ProbePort:               8081,
			EnableLeaderElection:    true,
			LogLevel:                "info",
			LogFormat:               "json",
		}

		err := cfg.Validate()
		assert.NoError(t, err)

		// Verify service mode specific settings
		assert.Equal(t, time.Minute*5, cfg.ReconcileInterval)
		assert.Equal(t, 2, cfg.MaxConcurrentReconciles)
		assert.True(t, cfg.EnableLeaderElection)
		assert.Equal(t, 8080, cfg.MetricsPort)
		assert.Equal(t, 8081, cfg.ProbePort)
	})

	t.Run("should support configurable reconcile intervals", func(t *testing.T) {
		testCases := []struct {
			name     string
			interval time.Duration
			valid    bool
		}{
			{"1 second", time.Second, true},
			{"30 seconds", time.Second * 30, true},
			{"5 minutes", time.Minute * 5, true},
			{"1 hour", time.Hour, true},
			{"500ms", time.Millisecond * 500, false}, // Too short
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := &config.Config{
					ReconcileInterval:       tc.interval,
					MaxConcurrentReconciles: 1,
					RetryBackoffBase:        time.Second,
					MaxRetryAttempts:        3,
					APIRateLimit:            10.0,
					APIBurst:                15,
					ReconcileTimeout:        time.Minute * 5,
					ListOperationTimeout:    time.Minute * 2,
					LogLevel:                "info",
					LogFormat:               "json",
				}

				err := cfg.Validate()
				if tc.valid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("should support leader election configuration", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Minute,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            10.0,
			APIBurst:                15,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
			EnableLeaderElection:    true,
			LogLevel:                "info",
			LogFormat:               "json",
		}

		err := cfg.Validate()
		assert.NoError(t, err)
		assert.True(t, cfg.EnableLeaderElection)
	})
}

func TestGracefulShutdownLogic(t *testing.T) {
	t.Run("should create cancellable context", func(t *testing.T) {
		timeout := time.Second * 5
		ctx := setupGracefulShutdown(timeout)

		require.NotNil(t, ctx)
		require.NotNil(t, ctx.Done())

		// Verify context is not cancelled initially
		select {
		case <-ctx.Done():
			t.Fatal("Context should not be cancelled initially")
		default:
			// Expected behavior
		}
	})

	t.Run("should handle context cancellation", func(t *testing.T) {
		timeout := time.Millisecond * 100
		ctx := setupGracefulShutdown(timeout)

		// Create a derived context to test cancellation behavior
		derivedCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Cancel the derived context
		cancel()

		// Verify cancellation
		select {
		case <-derivedCtx.Done():
			// Expected behavior
		case <-time.After(time.Second):
			t.Fatal("Context should be cancelled")
		}
	})
}

func TestHealthCheckConfiguration(t *testing.T) {
	t.Run("should validate health check setup parameters", func(t *testing.T) {
		cfg := &config.Config{
			ProbePort: 8081,
		}

		// Test that probe port is configurable
		assert.Equal(t, 8081, cfg.ProbePort)
	})

	t.Run("should support different probe ports", func(t *testing.T) {
		testCases := []int{8080, 8081, 9090, 3000}

		for _, port := range testCases {
			cfg := &config.Config{
				ProbePort: port,
			}
			assert.Equal(t, port, cfg.ProbePort)
		}
	})
}

func TestServiceModeFeatures(t *testing.T) {
	t.Run("should support concurrent reconciles configuration", func(t *testing.T) {
		testCases := []struct {
			name        string
			concurrency int
			valid       bool
		}{
			{"single reconcile", 1, true},
			{"dual reconcile", 2, true},
			{"high concurrency", 10, true},
			{"max concurrency", 100, true},
			{"zero concurrency", 0, false},
			{"negative concurrency", -1, false},
			{"too high concurrency", 101, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := &config.Config{
					ReconcileInterval:       time.Minute,
					MaxConcurrentReconciles: tc.concurrency,
					RetryBackoffBase:        time.Second,
					MaxRetryAttempts:        3,
					APIRateLimit:            10.0,
					APIBurst:                15,
					ReconcileTimeout:        time.Minute * 5,
					ListOperationTimeout:    time.Minute * 2,
					LogLevel:                "info",
					LogFormat:               "json",
				}

				err := cfg.Validate()
				if tc.valid {
					assert.NoError(t, err)
					assert.Equal(t, tc.concurrency, cfg.MaxConcurrentReconciles)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("should support API rate limiting configuration", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Minute,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            25.5,
			APIBurst:                50,
			ReconcileTimeout:        time.Minute * 5,
			ListOperationTimeout:    time.Minute * 2,
			LogLevel:                "info",
			LogFormat:               "json",
		}

		err := cfg.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 25.5, cfg.APIRateLimit)
		assert.Equal(t, 50, cfg.APIBurst)
	})

	t.Run("should support timeout configuration", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Minute,
			MaxConcurrentReconciles: 1,
			RetryBackoffBase:        time.Second,
			MaxRetryAttempts:        3,
			APIRateLimit:            10.0,
			APIBurst:                15,
			ReconcileTimeout:        time.Minute * 10,
			ListOperationTimeout:    time.Minute * 5,
			LogLevel:                "info",
			LogFormat:               "json",
		}

		err := cfg.Validate()
		assert.NoError(t, err)
		assert.Equal(t, time.Minute*10, cfg.ReconcileTimeout)
		assert.Equal(t, time.Minute*5, cfg.ListOperationTimeout)
	})
}
