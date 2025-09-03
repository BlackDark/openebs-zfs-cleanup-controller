package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the ZFSVolume cleanup controller
type Config struct {
	// Execution mode
	DryRun            bool
	ReconcileInterval time.Duration

	// Safety settings
	MaxConcurrentReconciles int
	RetryBackoffBase        time.Duration
	MaxRetryAttempts        int

	// Filtering options
	NamespaceFilter string
	LabelSelector   string

	// Server configuration
	MetricsPort          int
	ProbePort            int
	EnableLeaderElection bool

	// Logging
	LogLevel  string
	LogFormat string
}

// ValidationError represents a configuration validation error
type ValidationError struct {
	Field   string
	Value   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("invalid configuration for %s=%s: %s", e.Field, e.Value, e.Message)
}

// LoadConfig loads configuration from environment variables with defaults and validation
func LoadConfig() (*Config, error) {
	config := &Config{
		DryRun:                  getBoolEnv("DRY_RUN", false),
		ReconcileInterval:       getDurationEnv("RECONCILE_INTERVAL", time.Hour),
		MaxConcurrentReconciles: getIntEnv("MAX_CONCURRENT_RECONCILES", 1),
		RetryBackoffBase:        getDurationEnv("RETRY_BACKOFF_BASE", time.Second),
		MaxRetryAttempts:        getIntEnv("MAX_RETRY_ATTEMPTS", 3),
		NamespaceFilter:         getStringEnv("NAMESPACE_FILTER", ""),
		LabelSelector:           getStringEnv("LABEL_SELECTOR", ""),
		MetricsPort:             getIntEnv("METRICS_PORT", 8080),
		ProbePort:               getIntEnv("PROBE_PORT", 8081),
		EnableLeaderElection:    getBoolEnv("ENABLE_LEADER_ELECTION", false),
		LogLevel:                getStringEnv("LOG_LEVEL", "info"),
		LogFormat:               getStringEnv("LOG_FORMAT", "json"),
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// Validate performs comprehensive validation of all configuration fields
func (c *Config) Validate() error {
	var errors []ValidationError

	// Validate ReconcileInterval
	if c.ReconcileInterval <= 0 {
		errors = append(errors, ValidationError{
			Field:   "RECONCILE_INTERVAL",
			Value:   c.ReconcileInterval.String(),
			Message: "must be greater than 0",
		})
	}
	if c.ReconcileInterval < time.Second {
		errors = append(errors, ValidationError{
			Field:   "RECONCILE_INTERVAL",
			Value:   c.ReconcileInterval.String(),
			Message: "minimum value is 1s",
		})
	}

	// Validate MaxConcurrentReconciles
	if c.MaxConcurrentReconciles <= 0 {
		errors = append(errors, ValidationError{
			Field:   "MAX_CONCURRENT_RECONCILES",
			Value:   strconv.Itoa(c.MaxConcurrentReconciles),
			Message: "must be greater than 0",
		})
	}
	if c.MaxConcurrentReconciles > 100 {
		errors = append(errors, ValidationError{
			Field:   "MAX_CONCURRENT_RECONCILES",
			Value:   strconv.Itoa(c.MaxConcurrentReconciles),
			Message: "maximum value is 100",
		})
	}

	// Validate RetryBackoffBase
	if c.RetryBackoffBase <= 0 {
		errors = append(errors, ValidationError{
			Field:   "RETRY_BACKOFF_BASE",
			Value:   c.RetryBackoffBase.String(),
			Message: "must be greater than 0",
		})
	}
	if c.RetryBackoffBase < 100*time.Millisecond {
		errors = append(errors, ValidationError{
			Field:   "RETRY_BACKOFF_BASE",
			Value:   c.RetryBackoffBase.String(),
			Message: "minimum value is 100ms",
		})
	}

	// Validate MaxRetryAttempts
	if c.MaxRetryAttempts < 0 {
		errors = append(errors, ValidationError{
			Field:   "MAX_RETRY_ATTEMPTS",
			Value:   strconv.Itoa(c.MaxRetryAttempts),
			Message: "must be greater than or equal to 0",
		})
	}
	if c.MaxRetryAttempts > 10 {
		errors = append(errors, ValidationError{
			Field:   "MAX_RETRY_ATTEMPTS",
			Value:   strconv.Itoa(c.MaxRetryAttempts),
			Message: "maximum value is 10",
		})
	}

	// Validate LogLevel
	validLogLevels := []string{"debug", "info", "warn", "error"}
	if !contains(validLogLevels, strings.ToLower(c.LogLevel)) {
		errors = append(errors, ValidationError{
			Field:   "LOG_LEVEL",
			Value:   c.LogLevel,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(validLogLevels, ", ")),
		})
	}

	// Validate LogFormat
	validLogFormats := []string{"json", "text"}
	if !contains(validLogFormats, strings.ToLower(c.LogFormat)) {
		errors = append(errors, ValidationError{
			Field:   "LOG_FORMAT",
			Value:   c.LogFormat,
			Message: fmt.Sprintf("must be one of: %s", strings.Join(validLogFormats, ", ")),
		})
	}

	// Return first validation error if any exist
	if len(errors) > 0 {
		return errors[0]
	}

	return nil
}

// String returns a string representation of the configuration (excluding sensitive data)
func (c *Config) String() string {
	return fmt.Sprintf("Config{DryRun: %t, ReconcileInterval: %s, MaxConcurrentReconciles: %d, RetryBackoffBase: %s, MaxRetryAttempts: %d, NamespaceFilter: %q, LabelSelector: %q, MetricsPort: %d, ProbePort: %d, EnableLeaderElection: %t, LogLevel: %q, LogFormat: %q}",
		c.DryRun, c.ReconcileInterval, c.MaxConcurrentReconciles, c.RetryBackoffBase, c.MaxRetryAttempts, c.NamespaceFilter, c.LabelSelector, c.MetricsPort, c.ProbePort, c.EnableLeaderElection, c.LogLevel, c.LogFormat)
}

func getStringEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// contains checks if a slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
