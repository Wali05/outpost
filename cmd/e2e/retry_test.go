package e2e_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/hookdeck/outpost/internal/idgen"
)

func (s *basicSuite) TestRetry_FailedDeliveryAutoRetries() {
	tenant := s.createTenant()
	secret := testSecret
	dest := s.createWebhookDestination(tenant.ID, "*", withSecret(secret))

	s.publish(tenant.ID, "user.created", map[string]any{
		"test": "auto_retry",
	},
		withRetry(),
		withPublishMetadata(map[string]string{"should_err": "true"}),
	)

	// Wait for at least 2 delivery attempts (initial + retry)
	s.waitForNewMockServerEvents(dest.mockID, 2)

	// Wait for attempts to be logged
	attempts := s.waitForNewAttempts(tenant.ID, 2)
	s.Require().GreaterOrEqual(len(attempts), 2, "should have at least 2 attempts from automated retry")

	// Fetch in asc order and verify attempt_number increments
	var resp struct {
		Models []map[string]any `json:"models"`
	}
	status := s.doJSON(http.MethodGet, s.apiURL("/attempts?tenant_id="+tenant.ID+"&dir=asc"), nil, &resp)
	s.Require().Equal(http.StatusOK, status)
	s.Require().GreaterOrEqual(len(resp.Models), 2)

	for i, atm := range resp.Models {
		s.Equal(float64(i+1), atm["attempt_number"],
			"attempt %d should have attempt_number=%d (1-indexed, automated retry increments)", i, i+1)
	}
}

func (s *basicSuite) TestRetry_ManualRetryCreatesNewAttempt() {
	tenant := s.createTenant()
	dest := s.createWebhookDestination(tenant.ID, "*", withSecret(testSecret))

	eventID := idgen.Event()
	s.publish(tenant.ID, "user.created", map[string]any{
		"user_id": "456",
	}, withEventID(eventID))

	// Wait for initial delivery
	s.waitForNewAttempts(tenant.ID, 1)

	// Verify first attempt has attempt_number=1 (1-indexed)
	var attResp struct {
		Models []map[string]any `json:"models"`
	}
	status := s.doJSON(http.MethodGet, s.apiURL("/attempts?tenant_id="+tenant.ID+"&event_id="+eventID), nil, &attResp)
	s.Require().Equal(http.StatusOK, status)
	s.Require().NotEmpty(attResp.Models)
	s.Equal(float64(1), attResp.Models[0]["attempt_number"])

	// Manual retry
	retryStatus := s.retryEvent(eventID, dest.ID)
	s.Equal(http.StatusAccepted, retryStatus)

	// Wait for retry attempt
	s.waitForNewAttempts(tenant.ID, 2)

	// Verify: 2 attempts, one manual=true
	var verifyResp struct {
		Models []map[string]any `json:"models"`
	}
	status = s.doJSON(http.MethodGet, s.apiURL("/attempts?tenant_id="+tenant.ID+"&event_id="+eventID+"&dir=asc"), nil, &verifyResp)
	s.Require().Equal(http.StatusOK, status)
	s.Require().Len(verifyResp.Models, 2)

	// Attempt numbers should be sequential (1-indexed)
	for i, atm := range verifyResp.Models {
		s.Equal(float64(i+1), atm["attempt_number"],
			"attempt %d should have attempt_number=%d", i, i+1)
	}

	// Verify one manual=true
	manualCount := 0
	for _, atm := range verifyResp.Models {
		if manual, ok := atm["manual"].(bool); ok && manual {
			manualCount++
		}
	}
	s.Equal(1, manualCount, "should have exactly one manual retry attempt")
}

func (s *basicSuite) TestRetry_DuplicateManualRetryExecutesBoth() {
	tenant := s.createTenant()
	dest := s.createWebhookDestination(tenant.ID, "*", withSecret(testSecret))

	eventID := idgen.Event()
	s.publish(tenant.ID, "user.created", map[string]any{
		"test": "duplicate_manual_retry",
	}, withEventID(eventID))

	// Wait for initial delivery
	s.waitForNewAttempts(tenant.ID, 1)

	// First manual retry
	status := s.retryEvent(eventID, dest.ID)
	s.Equal(http.StatusAccepted, status)
	s.waitForNewAttempts(tenant.ID, 2)

	// Second manual retry for same event+destination
	status = s.retryEvent(eventID, dest.ID)
	s.Equal(http.StatusAccepted, status)
	s.waitForNewAttempts(tenant.ID, 3)

	// Verify: 3 attempts total, 2 manual
	var resp struct {
		Models []map[string]any `json:"models"`
	}
	status = s.doJSON(http.MethodGet, s.apiURL("/attempts?tenant_id="+tenant.ID+"&event_id="+eventID+"&dir=asc"), nil, &resp)
	s.Require().Equal(http.StatusOK, status)
	s.Require().Len(resp.Models, 3, "should have 3 attempts: initial + 2 manual retries")

	manualCount := 0
	for _, atm := range resp.Models {
		if manual, ok := atm["manual"].(bool); ok && manual {
			manualCount++
		}
	}
	s.Equal(2, manualCount, "should have exactly 2 manual retry attempts")
}

func (s *basicSuite) TestRetry_ManualRetryOnDisabledDestinationRejected() {
	tenant := s.createTenant()
	dest := s.createWebhookDestination(tenant.ID, "*")

	eventID := idgen.Event()
	s.publish(tenant.ID, "user.created", map[string]any{
		"test": "disabled_retry",
	}, withEventID(eventID))

	// Wait for delivery
	s.waitForNewAttempts(tenant.ID, 1)

	// Disable destination
	s.disableDestination(tenant.ID, dest.ID)

	// Retry should be rejected
	status := s.retryEvent(eventID, dest.ID)
	s.Equal(http.StatusBadRequest, status)
}

func (s *basicSuite) TestRetry_Webhook410NotRetried() {
	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusGone)
	}))
	s.T().Cleanup(receiver.Close)

	tenant := s.createTenant()
	dest := s.createWebhookDestination(tenant.ID, "*", withURL(receiver.URL))

	eventID := idgen.Event()
	s.publish(tenant.ID, "user.created", map[string]any{"test": "gone"}, withEventID(eventID), withRetry())

	attempts := s.waitForNewAttempts(tenant.ID, 1)
	s.Equal("410", attempts[0]["code"])

	// Automatic retries would land within a few seconds (interval 1s).
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		s.Require().EqualValues(1, hits.Load(), "410 must not be retried automatically")
		time.Sleep(100 * time.Millisecond)
	}

	// A manual retry is still allowed.
	s.Equal(http.StatusAccepted, s.retryEvent(eventID, dest.ID))
	s.waitForNewAttempts(tenant.ID, 2)
	s.EqualValues(2, hits.Load())
}
