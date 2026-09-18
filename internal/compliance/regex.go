package compliance

import (
	"regexp"
	"sync"
)

// ComplianceEngine executes high-performance pre-compiled regex rules
// for PII sanitization, anomaly detection, and regulatory compliance flags.
type ComplianceEngine struct {
	piiPattern         *regexp.Regexp
	washTradingPattern *regexp.Regexp
	suspiciousToken    *regexp.Regexp
}

var (
	instance *ComplianceEngine
	once     sync.Once
)

// NewComplianceEngine initializes pre-compiled regex patterns once (thread-safe).
func NewComplianceEngine() *ComplianceEngine {
	once.Do(func() {
		instance = &ComplianceEngine{
			// Regex for masking email addresses and IP addresses (PII scrubbing)
			piiPattern: regexp.MustCompile(`(?i)[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}|(?:\d{1,3}\.){3}\d{1,3}`),
			// Regex for identifying wash trading/manipulation keywords in tick metadata
			washTradingPattern: regexp.MustCompile(`(?i)(wash_trade|self_deal|circular_fill|spoofing|layering)`),
			// Regex for invalid or restricted symbol/token patterns
			suspiciousToken: regexp.MustCompile(`(?i)^(TEST_|RESTRICTED_|DEV_|MOCK_)`),
		}
	})
	return instance
}

// SanitizePayload scrubs PII (emails, IPs) from unstructured text payloads before LLM ingestion.
func (ce *ComplianceEngine) SanitizePayload(input string) string {
	if input == "" {
		return ""
	}
	return ce.piiPattern.ReplaceAllString(input, "[REDACTED_PII]")
}

// ValidateCompliance checks whether a trading pair or metadata contains compliance violations.
func (ce *ComplianceEngine) ValidateCompliance(symbol string, metadata string) (bool, string) {
	if ce.suspiciousToken.MatchString(symbol) {
		return false, "RESTRICTED_SYMBOL"
	}
	if ce.washTradingPattern.MatchString(metadata) {
		return false, "WASH_TRADING_FLAG"
	}
	return true, "PASSED"
}
