//go:build integration
// +build integration

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests require a real Kubernetes cluster and should be run manually with:
// go test -tags=integration -v ./cmd/cronjob/

func TestCronJobIntegrationExecution(t *testing.T) {
	tests := []struct {
		name         string
		envVars      map[string]string
		expectedExit int
		timeout      time.Duration
		checkOutput  func(t *testing.T, output string)
	}{
		{
			name: "successful dry run execution",
			envVars: map[string]string{
				"DRY_RUN":                   "true",
				"RECONCILE_INTERVAL":        "1h",
				"MAX_CONCURRENT_RECONCILES": "1",
				"RETRY_BACKOFF_BASE":        "1s",
				"MAX_RETRY_ATTEMPTS":        "3",
			},
			expectedExit: 0,
			timeout:      30 * time.Second,
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, "ZFSVolume Cleanup Controller starting")
				assert.Contains(t, output, "mode\": \"cronjob\"")
				assert.Contains(t, output, "DRY-RUN MODE ENABLED")
				assert.Contains(t, output, "cleanup completed successfully")
			},
		},
		{
			name: "execution with custom timeout",
			envVars: map[string]string{
				"DRY_RUN": "true",
			},
			expectedExit: 0,
			timeout:      30 * time.Second,
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, "timeout\": \"2m0s\"")
			},
		},
		{
			name: "invalid timeout format",
			envVars: map[string]string{
				"DRY_RUN": "true",
			},
			expectedExit: 1,
			timeout:      10 * time.Second,
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, "invalid timeout duration")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the cronjob binary
			binaryPath := buildCronJobBinary(t)
			defer os.Remove(binaryPath)

			// Prepare command arguments
			args := []string{}
			if tt.name == "execution with custom timeout" {
				args = append(args, "-timeout=2m")
			} else if tt.name == "invalid timeout format" {
				args = append(args, "-timeout=invalid")
			}

			// Execute the cronjob binary
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			cmd := exec.CommandContext(ctx, binaryPath, args...)

			// Set environment variables
			env := os.Environ()
			for key, value := range tt.envVars {
				env = append(env, key+"="+value)
			}
			cmd.Env = env

			output, err := cmd.CombinedOutput()
			outputStr := string(output)

			// Check exit code
			if exitError, ok := err.(*exec.ExitError); ok {
				assert.Equal(t, tt.expectedExit, exitError.ExitCode(),
					"Expected exit code %d, got %d. Output: %s",
					tt.expectedExit, exitError.ExitCode(), outputStr)
			} else if err != nil && tt.expectedExit != 0 {
				// Command failed to start or other error
				t.Logf("Command failed with error: %v, output: %s", err, outputStr)
			} else if err == nil && tt.expectedExit == 0 {
				// Success case
			} else {
				t.Fatalf("Unexpected error state: err=%v, expectedExit=%d, output=%s",
					err, tt.expectedExit, outputStr)
			}

			// Check output content
			if tt.checkOutput != nil {
				tt.checkOutput(t, outputStr)
			}

			t.Logf("Test output:\n%s", outputStr)
		})
	}
}

func TestCronJobIntegrationSignalHandling(t *testing.T) {
	// Build the cronjob binary
	binaryPath := buildCronJobBinary(t)
	defer os.Remove(binaryPath)

	tests := []struct {
		name         string
		signal       os.Signal
		expectedExit int
		timeout      time.Duration
	}{
		{
			name:         "SIGINT handling",
			signal:       syscall.SIGINT,
			expectedExit: 130,
			timeout:      15 * time.Second,
		},
		{
			name:         "SIGTERM handling",
			signal:       syscall.SIGTERM,
			expectedExit: 130,
			timeout:      15 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			// Start the cronjob with a long timeout to ensure it runs long enough for signal testing
			cmd := exec.CommandContext(ctx, binaryPath, "-timeout=5m")
			cmd.Env = append(os.Environ(),
				"DRY_RUN=true",
				"RECONCILE_INTERVAL=1h",
			)

			// Start the command
			err := cmd.Start()
			require.NoError(t, err, "Failed to start cronjob process")

			// Give the process a moment to start up
			time.Sleep(2 * time.Second)

			// Send the signal
			err = cmd.Process.Signal(tt.signal)
			require.NoError(t, err, "Failed to send signal to process")

			// Wait for the process to exit
			err = cmd.Wait()

			// Check the exit code
			if exitError, ok := err.(*exec.ExitError); ok {
				assert.Equal(t, tt.expectedExit, exitError.ExitCode(),
					"Expected exit code %d for signal %v, got %d",
					tt.expectedExit, tt.signal, exitError.ExitCode())
			} else {
				t.Fatalf("Expected process to exit with code %d, but got error: %v",
					tt.expectedExit, err)
			}
		})
	}
}

func TestCronJobIntegrationConfigurationLoading(t *testing.T) {
	// Build the cronjob binary
	binaryPath := buildCronJobBinary(t)
	defer os.Remove(binaryPath)

	tests := []struct {
		name        string
		envVars     map[string]string
		expectError bool
		checkOutput func(t *testing.T, output string)
	}{
		{
			name: "valid configuration",
			envVars: map[string]string{
				"DRY_RUN":                   "true",
				"RECONCILE_INTERVAL":        "30m",
				"MAX_CONCURRENT_RECONCILES": "2",
				"RETRY_BACKOFF_BASE":        "2s",
				"MAX_RETRY_ATTEMPTS":        "5",
				"NAMESPACE_FILTER":          "openebs",
				"LOG_LEVEL":                 "debug",
			},
			expectError: false,
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, "reconcileInterval\": \"30m0s\"")
				assert.Contains(t, output, "maxConcurrentReconciles\": 2")
				assert.Contains(t, output, "retryBackoffBase\": \"2s\"")
				assert.Contains(t, output, "maxRetryAttempts\": 5")
				assert.Contains(t, output, "namespaceFilter\": \"openebs\"")
			},
		},
		{
			name: "invalid reconcile interval",
			envVars: map[string]string{
				"DRY_RUN":            "true",
				"RECONCILE_INTERVAL": "invalid",
			},
			expectError: true,
			checkOutput: func(t *testing.T, output string) {
				assert.Contains(t, output, "failed to load configuration")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, binaryPath, "-timeout=10s")

			// Set environment variables
			env := os.Environ()
			for key, value := range tt.envVars {
				env = append(env, key+"="+value)
			}
			cmd.Env = env

			output, err := cmd.CombinedOutput()
			outputStr := string(output)

			if tt.expectError {
				assert.Error(t, err, "Expected command to fail but it succeeded")
			} else {
				// For successful cases, we might get exit code 0 or 1 depending on whether
				// there were any volumes to process or errors during processing
				if err != nil {
					if exitError, ok := err.(*exec.ExitError); ok {
						// Exit codes 0 and 1 are both acceptable for successful configuration loading
						assert.Contains(t, []int{0, 1}, exitError.ExitCode(),
							"Unexpected exit code: %d. Output: %s", exitError.ExitCode(), outputStr)
					}
				}
			}

			if tt.checkOutput != nil {
				tt.checkOutput(t, outputStr)
			}

			t.Logf("Test output:\n%s", outputStr)
		})
	}
}

// buildCronJobBinary builds the cronjob binary for testing
func buildCronJobBinary(t *testing.T) string {
	t.Helper()

	// Create a temporary binary path
	binaryPath := "/tmp/cronjob-test-" + strings.ReplaceAll(t.Name(), "/", "-")

	// Build the binary
	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	cmd.Dir = "." // Current directory should be cmd/cronjob

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to build cronjob binary: %s", string(output))

	return binaryPath
}
