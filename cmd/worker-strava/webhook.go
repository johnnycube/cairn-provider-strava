package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

// ---------------------------------------------------------------------------
// Strava webhook handlers
//
// Registered via workersdk.WebhookEvent/WebhookVerify in main.go. The
// cairn-server's generic webhook forwarder delivers raw POST bodies and
// GET-handshake requests on:
//
//	cairn.webhooks.strava.event   ← POSTs
//	cairn.webhooks.strava.verify  ← GETs (request/reply)
//
// All Strava-specific knowledge (envelope shape, verify_token semantics,
// owner_id → account_id mapping, hub.challenge response format) lives
// here. The core has zero awareness.
// ---------------------------------------------------------------------------

// stravaWebhookEnvelope is what the cairn-server's webhook forwarder
// publishes for every POST /webhooks/strava.
type stravaWebhookEnvelope struct {
	Provider string            `json:"provider"`
	Headers  map[string]string `json:"headers"`
	Body     []byte            `json:"body"`
}

// stravaEvent matches Strava's documented webhook payload:
// https://developers.strava.com/docs/webhooks/#event-data
type stravaEvent struct {
	ObjectType     string         `json:"object_type"` // "activity" | "athlete"
	ObjectID       int64          `json:"object_id"`
	AspectType     string         `json:"aspect_type"` // "create" | "update" | "delete"
	OwnerID        int64          `json:"owner_id"`
	SubscriptionID int64          `json:"subscription_id"`
	EventTime      int64          `json:"event_time"`
	Updates        map[string]any `json:"updates,omitempty"`
}

// handleWebhookEvent processes a single POSTed Strava webhook.
// Decodes the envelope+body, decides on the right downstream job/event
// based on aspect_type, and publishes via w.Enqueue.
func handleWebhookEvent(ctx context.Context, w *workersdk.Worker, ev workersdk.WebhookEvent) error {
	var env stravaWebhookEnvelope
	if err := json.Unmarshal(ev.Body, &env); err != nil {
		return &port.TerminalError{Reason: "bad_envelope", Cause: err}
	}

	var sev stravaEvent
	if err := json.Unmarshal(env.Body, &sev); err != nil {
		return &port.TerminalError{Reason: "bad_event_payload", Cause: err}
	}

	if sev.ObjectType != "activity" {
		// Strava sends athlete events too (e.g. deauthorize); we don't
		// handle those yet. ACK and move on.
		return nil
	}

	// Map owner_id → internal account_id via request/reply against the
	// server's lookup endpoint. This subject is generic
	// (cairn.accounts.lookup_by_provider_ext) so any worker can use it. We
	// also pass subscription_id: the same Strava athlete may be linked by
	// multiple Cairn connections, so subscription_id is the unambiguous key
	// (#50). The server prefers it and self-heals the mapping from the first
	// event, falling back to owner_id until then.
	accountID, userID, err := lookupAccountByOwnerID(ctx, w, "strava",
		fmt.Sprintf("%d", sev.OwnerID), fmt.Sprintf("%d", sev.SubscriptionID))
	if err != nil {
		// Orphaned webhook (account disconnected). ACK to drain the
		// Strava queue; reconcile-sync covers any drift later.
		return nil
	}

	switch sev.AspectType {
	case "create", "update":
		body, _ := json.Marshal(map[string]any{
			"account_id":    accountID,
			"user_id":       userID,
			"provider":      "strava",
			"ext_id":        fmt.Sprintf("%d", sev.ObjectID),
			"fetch_streams": true,
			"reason":        "webhook",
			"aspect_type":   sev.AspectType,
		})
		msgID := fmt.Sprintf("fetch:strava:%d:%d", sev.OwnerID, sev.ObjectID)
		return w.Enqueue(ctx, "cairn.jobs.fetch_source.strava", msgID, body)

	case "delete":
		body, _ := json.Marshal(map[string]any{
			"account_id": accountID,
			"user_id":    userID,
			"provider":   "strava",
			"ext_id":     fmt.Sprintf("%d", sev.ObjectID),
			"event_time": time.Unix(sev.EventTime, 0).UTC().Format(time.RFC3339),
		})
		msgID := fmt.Sprintf("source_deleted:strava:%d:%d", sev.OwnerID, sev.ObjectID)
		return w.Enqueue(ctx, "cairn.events.source.deleted_upstream", msgID, body)

	default:
		// Unknown aspect_type — log via the SDK's logger and ACK so we
		// don't retry forever on a payload shape we don't understand.
		return nil
	}
}

// handleWebhookVerify handles Strava's GET /webhooks/strava handshake.
// Echoes the hub.challenge if the verify_token matches the env-configured
// secret; rejects 403 otherwise.
func handleWebhookVerify(
	_ context.Context,
	_ *workersdk.Worker,
	req workersdk.WebhookVerifyRequest,
) (workersdk.WebhookVerifyResponse, error) {
	expected := os.Getenv("CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN")
	if expected == "" {
		return workersdk.WebhookVerifyResponse{
			Status:      http.StatusServiceUnavailable,
			ContentType: "text/plain",
			Body:        []byte("strava worker not configured: missing CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN"),
		}, nil
	}

	if req.Query["hub.mode"] != "subscribe" {
		return workersdk.WebhookVerifyResponse{
			Status:      http.StatusBadRequest,
			ContentType: "text/plain",
			Body:        []byte("unsupported hub.mode"),
		}, nil
	}
	if req.Query["hub.verify_token"] != expected {
		return workersdk.WebhookVerifyResponse{
			Status:      http.StatusForbidden,
			ContentType: "text/plain",
			Body:        []byte("invalid verify_token"),
		}, nil
	}

	body, err := json.Marshal(map[string]string{
		"hub.challenge": req.Query["hub.challenge"],
	})
	if err != nil {
		return workersdk.WebhookVerifyResponse{}, fmt.Errorf("marshal challenge: %w", err)
	}
	return workersdk.WebhookVerifyResponse{
		Status:      http.StatusOK,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

// lookupAccountByOwnerID asks the cairn-server to resolve a Strava webhook
// event into an internal account_id + user_id.
//
// Wire format on cairn.accounts.lookup_by_provider_ext:
//
//	request:  {"provider": "strava", "external_id": "12345", "subscription_id": "678"}
//	reply:    {"account_id": "uuid", "user_id": "uuid"} | {"error": "..."}
//
// subscription_id is the unambiguous key (#50) — the same athlete may be
// linked by multiple connections. The server prefers it and falls back to
// external_id (owner_id) when the subscription mapping isn't recorded yet.
// An empty subscriptionID is omitted from the request.
func lookupAccountByOwnerID(
	ctx context.Context,
	w *workersdk.Worker,
	provider, ownerID, subscriptionID string,
) (accountID, userID string, err error) {
	reqMap := map[string]string{
		"provider":    provider,
		"external_id": ownerID,
	}
	if subscriptionID != "" && subscriptionID != "0" {
		reqMap["subscription_id"] = subscriptionID
	}
	req, err := json.Marshal(reqMap)
	if err != nil {
		return "", "", err
	}
	resp, err := w.Request(ctx, "cairn.accounts.lookup_by_provider_ext", req, 1*time.Second)
	if err != nil {
		return "", "", err
	}
	var out struct {
		AccountID string `json:"account_id"`
		UserID    string `json:"user_id"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return "", "", err
	}
	if out.Error != "" {
		return "", "", errors.New(out.Error)
	}
	return out.AccountID, out.UserID, nil
}
