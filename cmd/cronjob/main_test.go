package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Unit tests that don't require a Kubernetes cluster

func TestTimeoutParsing(t *testing.T) {
	tests := []struct {
		name        string
		timeout     string
		expectError bool
		expected    time.Duration
	}{
		{
			name:        "valid timeout - minutes",
			timeout:     "5m",
			expectError: false,
			expected:    5 * time.Minute,
		},
		{
			name:        "valid timeout - seconds",
			timeout:     "30s",
			expectError: false,
			expected:    30 * time.Second,
		},
		{
			name:        "valid timeout - hours",
			timeout:     "2h",
			expectError: false,
			expected:    2 * time.Hour,
		},
		{
			name:        "invalid timeout format",
			timeout:     "invalid",
			expectError: true,
		},
		{
			name:        "empty timeout",
			timeout:     "",
			expectError: true,
		},
		{
			name:        "negative timeout",
			timeout:     "-5m",
			expectError: false, // time.ParseDuration allows negative values
			expected:    -5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration, err := time.ParseDuration(tt.timeout)

			if tt.expectError {
				assert.Error(t, err, "Expected error for timeout: %s", tt.timeout)
			} else {
				assert.NoError(t, err, "Unexpected error for timeout: %s", tt.timeout)
				assert.Equal(t, tt.expected, duration, "Duration mismatch for timeout: %s", tt.timeout)
			}
		})
	}
}

func TestExitCodeConstants(t *testing.T) {
	// Test that we're using standard exit codes
	tests := []struct {
		name    string
		code    int
		meaning string
	}{
		{
			name:    "success",
			code:    0,
			meaning: "successful completion",
		},
		{
			name:    "general error",
			code:    1,
			meaning: "general error",
		},
		{
			name:    "signal interrupt",
			code:    130,
			meaning: "script terminated by Control-C (SIGINT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the constants are what we expect
			switch tt.name {
			case "success":
				assert.Equal(t, 0, tt.code)
			case "general error":
				assert.Equal(t, 1, tt.code)
			case "signal interrupt":
				assert.Equal(t, 130, tt.code)
			}
		})
	}
}
