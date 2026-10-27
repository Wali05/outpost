package config

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hookdeck/outpost/internal/backoff"
	"github.com/hookdeck/outpost/internal/destregistry/providers/destwebhook"
)

const (
	defaultRetryIntervalSeconds = 30
	defaultRetryMaxLimit        = 10
)

// GetRetryBackoff returns the configured backoff strategy based on retry
// configuration. Every delay is at least one log flush, so the attempt a retry
// looks up is usually stored by then.
func (c *Config) GetRetryBackoff() (backoff.Backoff, int) {
	b, limit := c.retryBackoff()
	return c.floorBackoff(b), limit
}

func (c *Config) floorBackoff(b backoff.Backoff) backoff.Backoff {
	return &backoff.MinimumBackoff{Backoff: b, Min: time.Duration(c.LogBatchThresholdSeconds) * time.Second}
}

var (
	retryEnvKeys  = []string{"RETRY_SCHEDULE", "RETRY_INTERVAL_SECONDS", "MAX_RETRY_LIMIT"}
	retryYAMLKeys = []string{"retry_schedule", "retry_interval_seconds", "retry_max_limit"}
)

// retryConfigured reports whether the operator chose a retry policy: any retry
// key set in the config file or environment, or a value that differs from the
// defaults.
func (c *Config) retryConfigured() bool {
	return c.retryConfigSet ||
		len(c.RetrySchedule) > 0 ||
		c.RetryIntervalSeconds != defaultRetryIntervalSeconds ||
		c.RetryMaxLimit != defaultRetryMaxLimit
}

// webhookStandardRetry reports whether webhooks retry on the Standard Webhooks
// schedule: standard mode with no operator retry policy.
func (c *Config) webhookStandardRetry() bool {
	return c.Destinations.Webhook.Mode == "standard" && !c.retryConfigured()
}

// RetryTimingWarnings returns a warning when configured retry delays are below
// LOG_BATCH_THRESHOLD_SECONDS and so are raised to it. The configuration stays
// valid: existing schedules keep working, with the raised delays.
func (c *Config) RetryTimingWarnings() []string {
	floor := time.Duration(c.LogBatchThresholdSeconds) * time.Second
	if floor <= 0 {
		return nil
	}
	b, limit := c.retryBackoff()
	steps := limit
	if len(c.RetrySchedule) > 0 {
		steps = len(c.RetrySchedule)
	}
	var raised []string
	for i := 0; i < steps; i++ {
		d := b.Duration(i)
		// Exponential delays only grow (a non-positive value is overflow), so
		// the first delay at or above the floor ends the scan.
		if len(c.RetrySchedule) == 0 && (d >= floor || d <= 0) {
			break
		}
		if d < floor {
			raised = append(raised, d.String())
		}
	}
	if len(raised) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("retry delays below LOG_BATCH_THRESHOLD_SECONDS (%s) are raised to it: %s", floor, strings.Join(raised, ", "))}
}

func (c *Config) retryBackoff() (backoff.Backoff, int) {
	if len(c.RetrySchedule) > 0 {
		// Use scheduled backoff if retry_schedule is provided
		schedule := make([]time.Duration, len(c.RetrySchedule))
		for i, seconds := range c.RetrySchedule {
			schedule[i] = time.Duration(seconds) * time.Second
		}
		return &backoff.ScheduledBackoff{Schedule: schedule}, c.RetryMaxLimit
	}
	// Fall back to exponential backoff
	return &backoff.ExponentialBackoff{
		Interval: time.Duration(c.RetryIntervalSeconds) * time.Second,
		Base:     2,
	}, c.RetryMaxLimit
}

// defaultRetryPollBackoff is the ceiling for the auto retry poll backoff
// (RetryPollBackoffMs = 0): the monitor never sleeps longer than this while
// idle, even when the shortest retry delay in use is longer.
const defaultRetryPollBackoff = 30 * time.Second

// GetRetryPollBackoff returns the maximum time the retry monitor waits between
// polls while idle. An explicitly configured positive value is honored as-is.
// Otherwise (0 = auto) it is min(defaultRetryPollBackoff, shortest retry delay
// in use), so the monitor is always awake by the time the earliest
// possible retry comes due and the idle interval never adds latency.
func (c *Config) GetRetryPollBackoff() time.Duration {
	if c.RetryPollBackoffMs > 0 {
		return time.Duration(c.RetryPollBackoffMs) * time.Millisecond
	}
	shortest := time.Duration(c.RetryIntervalSeconds) * time.Second
	if len(c.RetrySchedule) > 0 {
		shortest = time.Duration(slices.Min(c.RetrySchedule)) * time.Second
	}
	if c.webhookStandardRetry() {
		shortest = min(shortest, max(destwebhook.StandardRetrySchedule[0], time.Duration(c.LogBatchThresholdSeconds)*time.Second))
	}
	if shortest > 0 && shortest < defaultRetryPollBackoff {
		return shortest
	}
	return defaultRetryPollBackoff
}

// yamlRetryValueEmpty treats null and an empty list like an unset env var.
func yamlRetryValueEmpty(v any) bool {
	list, isList := v.([]any)
	return v == nil || (isList && len(list) == 0)
}
