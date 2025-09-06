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
	if config.APIRateLimit != 10.0 {
		t.Errorf("Expected APIRateLimit to be 10.0, got %f", config.APIRateLimit)
	}
	if config.APIBurst != 15 {
		t.Errorf("Expected APIBurst to be 15, got %d", config.APIBurst)
	}
	if config.ReconcileTimeout != time.Minute*5 {
		t.Errorf("Expected ReconcileTimeout to be 5m, got %s", config.ReconcileTimeout)
	}
	if config.ListOperationTimeout != time.Minute*2 {
		t.Errorf("Expected ListOperationTimeout to be 2m, got %s", config.ListOperationTimeout)
	}
}

func TestLoadConfig_EnvironmentVariables(t *testing.T) {
	// Clear all environment variables first
	clearEnvVars()

	// Set environment variables
	_ = os.Setenv("DRY_RUN", "true")
	_ = os.Setenv("RECONCILE_INTERVAL", "30m")
	_ = os.Setenv("MAX_CONCURRENT_RECONCILES", "5")
	_ = os.Setenv("RETRY_BACKOFF_BASE", "2s")
	_ = os.Setenv("MAX_RETRY_ATTEMPTS", "5")
	_ = os.Setenv("NAMESPACE_FILTER", "openebs")
	_ = os.Setenv("LABEL_SELECTOR", "app=zfs")
	_ = os.Setenv("API_RATE_LIMIT", "25.5")
	_ = os.Setenv("API_BURST", "50")
	_ = os.Setenv("RECONCILE_TIMEOUT", "10m")
	_ = os.Setenv("LIST_OPERATION_TIMEOUT", "5m")
	_ = os.Setenv("LOG_LEVEL", "debug")
	_ = os.Setenv("LOG_FORMAT", "text")

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
	if config.APIRateLimit != 25.5 {
		t.Errorf("Expected APIRateLimit to be 25.5, got %f", config.APIRateLimit)
	}
	if config.APIBurst != 50 {
		t.Errorf("Expected APIBurst to be 50, got %d", config.APIBurst)
	}
	if config.ReconcileTimeout != 10*time.Minute {
		t.Errorf("Expected ReconcileTimeout to be 10m, got %s", config.ReconcileTimeout)
	}
	if config.ListOperationTimeout != 5*time.Minute {
		t.Errorf("Expected ListOperationTimeout to be 5m, got %s", config.ListOperationTimeout)
	}
}

func TestValidate_ValidConfiguration(t *testing.T) {
	config := validConfig()

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
		APIRateLimit:            25.5,
		APIBurst:                50,
		ReconcileTimeout:        10 * time.Minute,
		ListOperationTimeout:    5 * time.Minute,
		NamespaceFilter:         "openebs",
		LabelSelector:           "app=zfs",
		PVLabelSelector:         "pv.kubernetes.io/provisioned-by=zfs.csi.openebs.io",
		MetricsPort:             8080,
		ProbePort:               8081,
		EnableLeaderElection:    true,
		LogLevel:                "debug",
		LogFormat:               "text",
	}

	str := config.String()
	expected := `Config{DryRun: true, ReconcileInterval: 30m0s, MaxConcurrentReconciles: 5, RetryBackoffBase: 2s, MaxRetryAttempts: 5, APIRateLimit: 25.50, APIBurst: 50, ReconcileTimeout: 10m0s, ListOperationTimeout: 5m0s, NamespaceFilter: "openebs", LabelSelector: "app=zfs", PVLabelSelector: "pv.kubernetes.io/provisioned-by=zfs.csi.openebs.io", MetricsPort: 8080, ProbePort: 8081, EnableLeaderElection: true, LogLevel: "debug", LogFormat: "text"}`

	if str != expected {
		t.Errorf("Config.String() = %q, want %q", str, expected)
	}
}

func TestGetBoolEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	_ = os.Setenv("TEST_BOOL", "invalid")
	defer func() { _ = os.Unsetenv("TEST_BOOL") }()

	result := getBoolEnv("TEST_BOOL", true)
	if result != true {
		t.Errorf("getBoolEnv() with invalid value should return default, got %t", result)
	}
}

func TestGetIntEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	_ = os.Setenv("TEST_INT", "invalid")
	defer func() { _ = os.Unsetenv("TEST_INT") }()

	result := getIntEnv("TEST_INT", 42)
	if result != 42 {
		t.Errorf("getIntEnv() with invalid value should return default, got %d", result)
	}
}

func TestGetDurationEnv_InvalidValue(t *testing.T) {
	clearEnvVars()
	_ = os.Setenv("TEST_DURATION", "invalid")
	defer func() { _ = os.Unsetenv("TEST_DURATION") }()

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
		APIRateLimit:            10.0,
		APIBurst:                15,
		ReconcileTimeout:        time.Minute * 5,
		ListOperationTimeout:    time.Minute * 2,
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
		"API_RATE_LIMIT",
		"API_BURST",
		"RECONCILE_TIMEOUT",
		"LIST_OPERATION_TIMEOUT",
		"LOG_LEVEL",
		"LOG_FORMAT",
	}

	for _, env := range envVars {
		_ = os.Unsetenv(env)
	}
}
func TestValidate_InvalidAPIRateLimit(t *testing.T) {
	tests := []struct {
		name    string
		value   float64
		wantErr bool
	}{
		{"zero value", 0, true},
		{"negative value", -1.0, true},
		{"too large value", 1001.0, true},
		{"valid small value", 0.1, false},
		{"valid medium value", 10.0, false},
		{"valid large value", 1000.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.APIRateLimit = tt.value

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidAPIBurst(t *testing.T) {
	tests := []struct {
		name    string
		value   int
		wantErr bool
	}{
		{"zero value", 0, true},
		{"negative value", -1, true},
		{"too large value", 1001, true},
		{"valid small value", 1, false},
		{"valid medium value", 15, false},
		{"valid large value", 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.APIBurst = tt.value

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidReconcileTimeout(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		wantErr  bool
	}{
		{"zero duration", 0, true},
		{"negative duration", -time.Second, true},
		{"too small duration", 20 * time.Second, true},
		{"valid minimum duration", 30 * time.Second, false},
		{"valid large duration", time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.ReconcileTimeout = tt.duration

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InvalidListOperationTimeout(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		wantErr  bool
	}{
		{"zero duration", 0, true},
		{"negative duration", -time.Second, true},
		{"too small duration", 5 * time.Second, true},
		{"valid minimum duration", 10 * time.Second, false},
		{"valid large duration", time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := validConfig()
			config.ListOperationTimeout = tt.duration

			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetFloat64Env_ValidValue(t *testing.T) {
	clearEnvVars()
	_ = os.Setenv("TEST_FLOAT", "25.5")
	defer func() { _ = os.Unsetenv("TEST_FLOAT") }()

	result := getFloat64Env("TEST_FLOAT", 10.0)
	if result != 25.5 {
		t.Errorf("getFloat64Env() = %f, want 25.5", result)
	}
}

func TestGetFloat64Env_InvalidValue(t *testing.T) {
	clearEnvVars()
	_ = os.Setenv("TEST_FLOAT", "invalid")
	defer func() { _ = os.Unsetenv("TEST_FLOAT") }()

	result := getFloat64Env("TEST_FLOAT", 42.5)
	if result != 42.5 {
		t.Errorf("getFloat64Env() with invalid value should return default, got %f", result)
	}
}

func TestGetFloat64Env_EmptyValue(t *testing.T) {
	clearEnvVars()

	result := getFloat64Env("NONEXISTENT_FLOAT", 99.9)
	if result != 99.9 {
		t.Errorf("getFloat64Env() with empty value should return default, got %f", result)
	}
}
