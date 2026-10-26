package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hookdeck/outpost/internal/backoff"
	"github.com/hookdeck/outpost/internal/mcpevents"
)

const maxDurationSeconds = math.MaxInt64 / int64(time.Second)

type MCPConfig struct {
	Enabled                       bool      `yaml:"enabled" env:"MCP_ENABLED" desc:"If true, registers the experimental 'mcp' destination type (MCP Events webhook subscriptions). Admin API key only. Delete mcp destinations before turning it off; while it is off, they stay listed and each event fails once without retries." required:"N" default:"false"`
	AllowInsecureCallbacks        bool      `yaml:"allow_insecure_callbacks" env:"MCP_ALLOW_INSECURE_CALLBACKS" desc:"If true, allows http callback URLs whose addresses are all in DESTINATIONS_PRIVATE_ADDRESS_ALLOWLIST. Without it, http is only allowed for loopback addresses in the allowlist. Development only." required:"N" default:"false"`
	RetrySchedule                 []int     `yaml:"retry_schedule" env:"MCP_RETRY_SCHEDULE" envSeparator:"," desc:"Comma-separated retry delays in seconds for mcp deliveries (in YAML, a list). Its length is the number of retries. RETRY_SCHEDULE, RETRY_INTERVAL_SECONDS and MAX_RETRY_LIMIT don't apply to mcp. Delays shorter than LOG_BATCH_THRESHOLD_SECONDS are raised to it." required:"N" default:"30,120,600"`
	VerificationTTLSeconds        int       `yaml:"verification_ttl_seconds" env:"MCP_VERIFICATION_TTL_SECONDS" desc:"How long, in seconds, a passed callback verification challenge is cached per tenant, principal and callback URL." required:"N" default:"86400"`
	SubscriptionTTLDefaultSeconds int       `yaml:"subscription_ttl_default_seconds" env:"MCP_SUBSCRIPTION_TTL_DEFAULT_SECONDS" desc:"Lifetime, in seconds, of a subscription made through the MCP subscribe endpoint when the request has no ttlMs. Must be between the min and max." required:"N" default:"3600"`
	SubscriptionTTLMinSeconds     int       `yaml:"subscription_ttl_min_seconds" env:"MCP_SUBSCRIPTION_TTL_MIN_SECONDS" desc:"Shortest lifetime, in seconds, granted through the MCP subscribe endpoint; shorter requests are raised to it." required:"N" default:"300"`
	SubscriptionTTLMaxSeconds     int       `yaml:"subscription_ttl_max_seconds" env:"MCP_SUBSCRIPTION_TTL_MAX_SECONDS" desc:"Longest lifetime, in seconds, granted through the MCP subscribe endpoint; longer requests and a null ttlMs get this." required:"N" default:"86400"`
	Events                        MCPEvents `yaml:"events" desc:"Event definitions keyed by topic name (a YAML mapping): an optional description and a payload_schema (JSON Schema 2020-12 for the event data, an object). Subscriptions must target a defined event; their arguments are checked against the inputSchema inferred from the schema. Every key must be in TOPICS when TOPICS is set. Ignored when MCP_EVENTS_FILE is set." required:"N"`
	EventsFile                    string    `yaml:"events_file" env:"MCP_EVENTS_FILE" desc:"Path to a YAML or JSON file (JSON when the name ends in .json) with event definitions in the mcp.events shape. Takes precedence over mcp.events." required:"N"`
}

func (c *MCPConfig) initDefaults() {
	*c = MCPConfig{
		RetrySchedule:          []int{30, 120, 600},
		VerificationTTLSeconds: 86400,

		SubscriptionTTLDefaultSeconds: 3600,
		SubscriptionTTLMinSeconds:     300,
		SubscriptionTTLMaxSeconds:     86400,
	}
}

func invalidMCP(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMCPConfig, fmt.Sprintf(format, args...))
}

func (c *Config) validateMCP() error {
	m := &c.MCP
	if !m.Enabled {
		c.mcpCatalog = nil
		return nil
	}
	if m.VerificationTTLSeconds < 1 || int64(m.VerificationTTLSeconds) > maxDurationSeconds {
		return invalidMCP("MCP_VERIFICATION_TTL_SECONDS (mcp.verification_ttl_seconds) must be between 1 and %d, got %d", maxDurationSeconds, m.VerificationTTLSeconds)
	}
	if len(m.RetrySchedule) == 0 {
		return invalidMCP("MCP_RETRY_SCHEDULE (mcp.retry_schedule) must list at least one delay")
	}
	for i, seconds := range m.RetrySchedule {
		if seconds < 1 {
			return invalidMCP("MCP_RETRY_SCHEDULE (mcp.retry_schedule) entries must be at least 1 second, got %d at index %d", seconds, i)
		}
		if int64(seconds) > maxDurationSeconds {
			return invalidMCP("MCP_RETRY_SCHEDULE (mcp.retry_schedule) entry %d at index %d is too large", seconds, i)
		}
	}
	if err := m.validateSubscriptionTTL(); err != nil {
		return err
	}
	return c.validateMCPEvents()
}

func (m *MCPConfig) validateSubscriptionTTL() error {
	for _, v := range []struct {
		env, key string
		seconds  int
	}{
		{"MCP_SUBSCRIPTION_TTL_DEFAULT_SECONDS", "mcp.subscription_ttl_default_seconds", m.SubscriptionTTLDefaultSeconds},
		{"MCP_SUBSCRIPTION_TTL_MIN_SECONDS", "mcp.subscription_ttl_min_seconds", m.SubscriptionTTLMinSeconds},
		{"MCP_SUBSCRIPTION_TTL_MAX_SECONDS", "mcp.subscription_ttl_max_seconds", m.SubscriptionTTLMaxSeconds},
	} {
		if v.seconds < 1 || int64(v.seconds) > maxDurationSeconds {
			return invalidMCP("%s (%s) must be between 1 and %d, got %d", v.env, v.key, maxDurationSeconds, v.seconds)
		}
	}
	if m.SubscriptionTTLMinSeconds > m.SubscriptionTTLDefaultSeconds || m.SubscriptionTTLDefaultSeconds > m.SubscriptionTTLMaxSeconds {
		return invalidMCP("MCP_SUBSCRIPTION_TTL_DEFAULT_SECONDS (mcp.subscription_ttl_default_seconds) must be between MCP_SUBSCRIPTION_TTL_MIN_SECONDS (%d) and MCP_SUBSCRIPTION_TTL_MAX_SECONDS (%d), got %d",
			m.SubscriptionTTLMinSeconds, m.SubscriptionTTLMaxSeconds, m.SubscriptionTTLDefaultSeconds)
	}
	return nil
}

// MCPSubscriptionTTL returns the lifetime rules of the MCP subscribe endpoint,
// or nil when MCP is off.
func (c *Config) MCPSubscriptionTTL() *mcpevents.TTLConfig {
	if !c.MCP.Enabled {
		return nil
	}
	return &mcpevents.TTLConfig{
		Default: time.Duration(c.MCP.SubscriptionTTLDefaultSeconds) * time.Second,
		Min:     time.Duration(c.MCP.SubscriptionTTLMinSeconds) * time.Second,
		Max:     time.Duration(c.MCP.SubscriptionTTLMaxSeconds) * time.Second,
	}
}

func (c *Config) MCPRetryBackoff() (backoff.Backoff, int) {
	schedule := make([]time.Duration, len(c.MCP.RetrySchedule))
	for i, seconds := range c.MCP.RetrySchedule {
		schedule[i] = time.Duration(seconds) * time.Second
	}
	return c.floorBackoff(&backoff.ScheduledBackoff{Schedule: schedule}), len(schedule)
}

func (c *Config) MCPWarnings() []string {
	if !c.MCP.Enabled {
		return nil
	}
	var warnings []string
	if c.MCP.AllowInsecureCallbacks {
		if len(c.Destinations.PrivateAddressAllowlist) == 0 {
			warnings = append(warnings, "MCP_ALLOW_INSECURE_CALLBACKS is true but DESTINATIONS_PRIVATE_ADDRESS_ALLOWLIST is empty, so it has no effect.")
		} else {
			warnings = append(warnings, "MCP_ALLOW_INSECURE_CALLBACKS is true: plain http callback URLs are accepted for allowlisted addresses, sending event payloads unencrypted. Use it for development only.")
		}
	}
	if floor := time.Duration(c.LogBatchThresholdSeconds) * time.Second; floor > 0 {
		var raised []string
		for _, seconds := range c.MCP.RetrySchedule {
			if d := time.Duration(seconds) * time.Second; d < floor {
				raised = append(raised, d.String())
			}
		}
		if len(raised) > 0 {
			warnings = append(warnings, fmt.Sprintf("MCP_RETRY_SCHEDULE: delays below LOG_BATCH_THRESHOLD_SECONDS (%s) are raised to it: %s", floor, strings.Join(raised, ", ")))
		}
	}
	return warnings
}
