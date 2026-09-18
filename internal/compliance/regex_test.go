package compliance

import (
	"testing"
)

func TestSanitizePayload(t *testing.T) {
	engine := NewComplianceEngine()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Scrubs email address",
			input:    "User trader@example.com executed order from 192.168.1.50",
			expected: "User [REDACTED_PII] executed order from [REDACTED_PII]",
		},
		{
			name:     "Clean string unchanged",
			input:    "Standard order BTC-USD bid=65000 ask=65100",
			expected: "Standard order BTC-USD bid=65000 ask=65100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.SanitizePayload(tt.input)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestValidateCompliance(t *testing.T) {
	engine := NewComplianceEngine()

	tests := []struct {
		name          string
		symbol        string
		metadata      string
		expectedValid bool
		expectedReason string
	}{
		{
			name:          "Valid pair and metadata",
			symbol:        "BTC-USD",
			metadata:      "normal market order",
			expectedValid: true,
			expectedReason: "PASSED",
		},
		{
			name:          "Restricted symbol prefix",
			symbol:        "RESTRICTED_ETH-USD",
			metadata:      "normal order",
			expectedValid: false,
			expectedReason: "RESTRICTED_SYMBOL",
		},
		{
			name:          "Wash trading metadata flag",
			symbol:        "ETH-USD",
			metadata:      "detected potential wash_trade pattern",
			expectedValid: false,
			expectedReason: "WASH_TRADING_FLAG",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, reason := engine.ValidateCompliance(tt.symbol, tt.metadata)
			if valid != tt.expectedValid || reason != tt.expectedReason {
				t.Errorf("expected (%v, %q), got (%v, %q)", tt.expectedValid, tt.expectedReason, valid, reason)
			}
		})
	}
}
