package destwebhook_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hookdeck/outpost/internal/destregistry"
	"github.com/hookdeck/outpost/internal/destregistry/providers/destwebhook"
	"github.com/hookdeck/outpost/internal/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHTTPResponse_MaxBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		maxBytes int
		wantBody string
	}{
		{
			name:     "no limit stores verbatim",
			body:     strings.Repeat("x", 5000),
			maxBytes: 0,
			wantBody: strings.Repeat("x", 5000),
		},
		{
			name:     "under limit stores verbatim",
			body:     "hello world",
			maxBytes: 1024,
			wantBody: "hello world",
		},
		{
			name:     "exactly at limit stores verbatim",
			body:     strings.Repeat("x", 1024),
			maxBytes: 1024,
			wantBody: strings.Repeat("x", 1024),
		},
		{
			name:     "over limit replaced with placeholder",
			body:     strings.Repeat("x", 2048),
			maxBytes: 1024,
			wantBody: "Response body exceeded 1024 bytes and was not stored",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}
			delivery := &destregistry.Delivery{}

			destwebhook.ParseHTTPResponse(delivery, resp, tt.maxBytes)

			assert.Equal(t, http.StatusOK, delivery.Response["status"], "status should be preserved")
			assert.Equal(t, tt.wantBody, delivery.Response["body"])
		})
	}
}

func TestPublish_NonRetryableStatuses(t *testing.T) {
	t.Parallel()

	providers := map[string]func(t *testing.T) *destwebhook.WebhookDestination{
		"default":  func(t *testing.T) *destwebhook.WebhookDestination { return NewTestProvider(t) },
		"standard": func(t *testing.T) *destwebhook.WebhookDestination { return newStandardProvider(t) },
	}
	statuses := []struct {
		status       int
		nonRetryable bool
	}{
		{http.StatusGone, true},
		{http.StatusRequestEntityTooLarge, true},
		{http.StatusBadRequest, false},
		{http.StatusNotFound, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
	}

	for mode, newProvider := range providers {
		for _, tc := range statuses {
			t.Run(fmt.Sprintf("%s/%d", mode, tc.status), func(t *testing.T) {
				t.Parallel()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
				}))
				t.Cleanup(server.Close)

				destination := testutil.DestinationFactory.Any(
					testutil.DestinationFactory.WithType("webhook"),
					testutil.DestinationFactory.WithConfig(map[string]string{"url": server.URL}),
					testutil.DestinationFactory.WithCredentials(map[string]string{"secret": standardSecret}),
				)
				publisher, err := newProvider(t).CreatePublisher(context.Background(), &destination)
				require.NoError(t, err)
				event := testutil.EventFactory.Any()

				delivery, err := publisher.Publish(context.Background(), &event)

				require.Error(t, err)
				assert.Equal(t, "failed", delivery.Status)
				assert.Equal(t, fmt.Sprint(tc.status), delivery.Code)
				var pubErr *destregistry.ErrDestinationPublishAttempt
				require.ErrorAs(t, err, &pubErr)
				assert.Equal(t, tc.nonRetryable, pubErr.NonRetryable)
				assert.False(t, pubErr.SkipFailureCount)
			})
		}
	}
}
