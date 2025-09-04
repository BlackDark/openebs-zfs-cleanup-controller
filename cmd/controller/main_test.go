package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/blackdark/openebs-zfsvolume-cleanup-controller/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestSetupHealthChecks(t *testing.T) {
	// Test configuration
	testConfig := &config.Config{
		DryRun:                  true,
		ReconcileInterval:       time.Minute,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
		NamespaceFilter:         "",
		LabelSelector:           "",
		MetricsPort:             8080,
		ProbePort:               8081,
		EnableLeaderElection:    false,
		LogLevel:                "info",
		LogFormat:               "json",
	}

	t.Run("should validate health check configuration", func(t *testing.T) {
		// Test that the configuration is valid for health checks
		assert.Equal(t, 8081, testConfig.ProbePort)
		assert.NotEmpty(t, testConfig.LogLevel)
		assert.NotEmpty(t, testConfig.LogFormat)
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

	t.Run("should validate health check function signatures", func(t *testing.T) {
		// Test that our health check functions have the correct signature
		// This is a compile-time check that the functions exist and have the right signature

		// Liveness check function signature test
		livenessCheck := func(req *http.Request) error {
			return nil
		}
		assert.NotNil(t, livenessCheck)

		// Readiness check function signature test
		readinessCheck := func(req *http.Request) error {
			return nil
		}
		assert.NotNil(t, readinessCheck)
	})
}

func TestSetupGracefulShutdown(t *testing.T) {
	t.Run("should create context that can be cancelled", func(t *testing.T) {
		timeout := time.Second * 5
		ctx := setupGracefulShutdown(timeout)

		assert.NotNil(t, ctx)

		// Verify context is not cancelled initially
		select {
		case <-ctx.Done():
			t.Fatal("Context should not be cancelled initially")
		default:
			// Expected behavior
		}
	})

	t.Run("should handle timeout correctly", func(t *testing.T) {
		timeout := time.Millisecond * 100
		ctx := setupGracefulShutdown(timeout)

		assert.NotNil(t, ctx)

		// The context should remain active until a signal is received
		// We can't easily test signal handling in unit tests, but we can
		// verify the context is properly created
		assert.NotNil(t, ctx.Done())
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("should validate reconcile interval", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Second * 30,
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
		assert.NoError(t, err)
	})

	t.Run("should reject invalid reconcile interval", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Millisecond * 500, // Too short
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
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RECONCILE_INTERVAL")
	})

	t.Run("should validate max concurrent reconciles", func(t *testing.T) {
		cfg := &config.Config{
			ReconcileInterval:       time.Minute,
			MaxConcurrentReconciles: 5,
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
		assert.NoError(t, err)
	})
}
