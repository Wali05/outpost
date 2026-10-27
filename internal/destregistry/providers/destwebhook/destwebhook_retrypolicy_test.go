package destwebhook_test

import (
	"testing"
	"time"

	"github.com/hookdeck/outpost/internal/backoff"
	"github.com/hookdeck/outpost/internal/destregistry/providers/destwebhook"
	"github.com/stretchr/testify/assert"
)

func TestWebhookRetryPolicy(t *testing.T) {
	t.Parallel()

	_, _, ok := NewTestProvider(t).RetryPolicy()
	assert.False(t, ok, "no policy unless configured")

	b, maxRetries, ok := NewTestProvider(t,
		destwebhook.WithRetryPolicy(&backoff.ConstantBackoff{Interval: time.Minute}, 4),
	).RetryPolicy()
	assert.True(t, ok)
	assert.Equal(t, 4, maxRetries)
	assert.Equal(t, time.Minute, b.Duration(0))
}
