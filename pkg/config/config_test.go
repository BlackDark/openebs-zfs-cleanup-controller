package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear all environment variables
	clearEnvVars()

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// Verify default values
	if config.DryRun != false {
		t.Errorf("Expected DryRun to be false, got %t", config.DryRun)
	}
	if config.ReconcileInterval != time.Hour {
		t.Errorf("Expected ReconcileInterval to be 1h, got %s", config.ReconcileInterval)
	}
	if config.MaxConcurrentReconciles != 1 {
		t.Errorf("Expected MaxConcurrentReconciles to be 1, got %d", config.MaxConcurrentReconciles)
	}
	if config.RetryBackoffBase != time.Second {
		t.Errorf("Expected RetryBackoffBase to be 1s, got %s", config.RetryBackoffBase)
	}
	if config.MaxRetryAttempts != 3 {
		t.Errorf("Expected MaxRetryAttempts to be 3, got %d", config.MaxRetryAttempts)
	}
	if config.NamespaceFilter != "" {
		t.Errorf("Expected NamespaceFilter to be empty, got %q", config.NamespaceFilter)
	}
	if config.LabelSelector != "" {
		t.Errorf("Expected LabelSelector to be empty, got %q", config.LabelSelector)
	}
	if config.LogLevel != "info" {
		t.Errorf("Expected LogLevel to be 'info', got %q", config.LogLevel)
	}
	if config.LogFormat != "json" {
		t.Errorf("Expected LogFormat to be 'json', got %q", config.LogFormat)
	}
}

func TestLoadConfig_EnvironmentVariables(t *testing.T) {
	// Clear all environment variables first
	clearEnvVars()

	// Set environment variables
	os.Setenv("DRY_RUN", "true")
	os.Setenv("RECONCILE_INTERVAL", "30m")
	os.Setenv("MAX_CONCURRENT_RECONCILES", "5")
	os.Setenv("RETRY_BACKOFF_BASE", "2s")
	os.Setenv("MAX_RETRY_ATTEMPTS", "5")
	os.Setenv("NAMESPACE_FILTER", "openebs")
	os.Setenv("LABEL_SELECTOR", "app=zfs")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "text")

	defer clearEnvVars()

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// Verify environment variable values
	if config.DryRun != true {
		t.Errorf("Expected DryRun to be true, got %t", config.DryRun)
	}
	if config.ReconcileInterval != 30*time.Minute {
		t.Errorf("Expected ReconcileInterval to be 30m, got %s", config.ReconcileInterval)
	}
	if config.MaxConcurrentReconciles != 5 {
		t.Errorf("Expected MaxConcurrentReconciles to be 5, got %d", config.MaxConcurrentReconciles)
	}
	if config.RetryBackoffBase != 2*time.Second {
		t.Errorf("Expected RetryBackoffBase to be 2s, got %s", config.RetryBackoffBase)
	}
	if config.MaxRetryAttempts != 5 {
		t.Errorf("Expected MaxRetryAttempts to be 5, got %d", config.MaxRetryAttempts)
	}
	if config.NamespaceFilter != "openebs" {
		t.Errorf("Expected NamespaceFilter to be 'openebs', got %q", config.NamespaceFilter)
	}
	if config.LabelSelector != "app=zfs" {
		t.Errorf("Expected LabelSelector to be 'app=zfs', got %q", config.LabelSelector)
	}
	if config.LogLevel != "debug" {
		t.Errorf("Expected LogLevel to be 'debug', got %q", config.LogLevel)
	}
	if config.LogFormat != "text" {
		t.Errorf("Expected LogFormat to be 'text', got %q", config.LogFormat)
	}
}

func TestValidate_ValidConfiguration(t *testing.T) {
	config := &Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		NamespaceFilter:         "",
		LabelSelector:           "",
		LogLevel:                "info",
		LogFormat:               "json",
	}

	if err := config.Validate(); err != nil {
		t.Errorf("Validate() failed for valid configuration: %v", err)
	}
}

func TestValidate_InvalidReconcileInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		wantErr  bool
	}{
		{"zero interval", 0, true},
		{"negative interval", -time.Hour, true},
		{"too small interval", 500 * time.Millisecond, true},
		{"valid interval", time.Second, false},
		{"valid large interval", time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.ReconcileInterval = tt.interval

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidMaxConcurrentReconciles(t *testing.T) {
	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"zero value", 0, true},
		{"negative value", -1, true},
		{"too large value", 101, true},
		{"valid small value", 1, false},
		{"valid large value", 100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.MaxConcurrentReconciles = tt.value

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidRetryBackoffBase(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		wantErr  bool
	}{
		{"zero duration", 0, true},
		{"negative duration", -time.Second, true},
		{"too small duration", 50 * time.Millisecond, true},
		{"valid small duration", 100 * time.Millisecond, false},
		{"valid large duration", time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.RetryBackoffBase = tt.duration

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidMaxRetryAttempts(t *testing.T) {
	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"negative value", -1, true},
		{"too large value", 11, true},
		{"valid zero value", 0, false},
		{"valid small value", 3, false},
		{"valid large value", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.MaxRetryAttempts = tt.value

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		logLevel string
		wantErr  bool
	}{
		{"invalid level", "invalid", true},
		{"empty level", "", true},
		{"uppercase valid level", "INFO", false},
		{"mixed case valid level", "Debug", false},
		{"valid debug", "debug", false},
		{"valid info", "info", false},
		{"valid warn", "warn", false},
		{"valid error", "error", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.LogLevel = tt.logLevel

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidLogFormat(t *testing.T) {
	tests := []struct {
		name      string
		logFormat string
		wantErr   bool
	}{
		{"invalid format", "invalid", true},
		{"empty format", "", true},
		{"uppercase valid format", "JSON", false},
		{"mixed case valid format", "Text", false},
		{"valid json", "json", false},
		{"valid text", "text", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.LogFormat = tt.logFormat

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	err := ValidationError{
		Field:   "TEST_FIELD",
		Value:   "invalid_value",
		Message: "test error message",
	}

	expected := "invalid configuration for TEST_FIELD=invalid_value: test error message"
	if err.Error() != expected {
		t.Errorf("ValidationError.Error() = %q, want %q", err.Error(), expected)
	}
}

func TestConfig_String(t *testing.T) {
	config := &Config{
		DryRun:                  true,
		ReconcileInterval:       30 * time.Minute,
		MaxConcurrentReconciles: 5,
		RetryBackoffBase:        2 * time.Second,
		MaxRetryAttempts:        5,
		NamespaceFilter:         "openebs",
		LabelSelector:           "app=zfs",
		MetricsPort:             8080,
		ProbePort:               8081,
		EnableLeaderElection:    true,
		LogLevel:                "debug",
		LogFormat:               "text",
	}

	str := config.String()
	expected := `Config{DryRun: true, ReconcileInterval: 30m0s, MaxConcurrentReconciles: 5, RetryBackoffBase: 2s, MaxRetryAttempts: 5, NamespaceFilter: "openebs", LabelSelector: "app=zfs", MetricsPort: 8080, ProbePort: 8081, EnableLeaderElection: true, LogLevel: "debug", LogFormat: "text"}`

	if str != expected {
		t.Errorf("Config.String() = %q, want %q", str, expected)
	}
}

func TestGetBoolEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	os.Setenv("TEST_BOOL", "invalid")
	defer os.Unsetenv("TEST_BOOL")

	result := getBoolEnv("TEST_BOOL", true)
	if result != true {
		t.Errorf("getBoolEnv() with invalid value should return default, got %t", result)
	}
}

func TestGetIntEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	os.Setenv("TEST_INT", "invalid")
	defer os.Unsetenv("TEST_INT")

	result := getIntEnv("TEST_INT", 42)
	if result != 42 {
		t.Errorf("getIntEnv() with invalid value should return default, got %d", result)
	}
}

func TestGetDurationEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	os.Setenv("TEST_DURATION", "invalid")
	defer os.Unsetenv("TEST_DURATION")

	result := getDurationEnv("TEST_DURATION", time.Hour)
	if result != time.Hour {
		t.Errorf("getDurationEnv() with invalid value should return default, got %s", result)
	}
}

// Helper functions

func validConfig() *Config {
	return &Config{
		DryRun:                  false,
		ReconcileInterval:       time.Hour,
		MaxConcurrentReconciles: 1,
		RetryBackoffBase:        time.Second,
		MaxRetryAttempts:        3,
		NamespaceFilter:         "",
		LabelSelector:           "",
		LogLevel:                "info",
		LogFormat:               "json",
	}
}

func clearEnvVars() {
	envVars := []string{
		"DRY_RUN",
		"RECONCILE_INTERVAL",
		"MAX_CONCURRENT_RECONCILES",
		"RETRY_BACKOFF_BASE",
		"MAX_RETRY_ATTEMPTS",
		"NAMESPACE_FILTER",
		"LABEL_SELECTOR",
		"LOG_LEVEL",
		"LOG_FORMAT",
	}

	for _, env := range envVars {
		os.Unsetenv(env)
	}
}
