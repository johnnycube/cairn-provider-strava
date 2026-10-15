package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

// webhookEvent wraps a Strava event in the server's forwarder envelope.
func webhookEvent(t *testing.T, ev stravaEvent) workersdk.WebhookEvent {
	t.Helper()
	inner, _ := json.Marshal(ev)
	body, err := json.Marshal(stravaWebhookEnvelope{Provider: "strava", Body: inner})
	if err != nil {
		t.Fatal(err)
	}
	return workersdk.WebhookEvent{Body: body}
}

// withLookup answers account lookups with reply and records the requests.
func withLookup(t *testing.T, h *harness, reply map[string]string) *[]map[string]string {
	t.Helper()
	var reqs []map[string]string
	if _, err := h.bus.RespondTo(context.Background(), "cairn.accounts.lookup_by_provider_ext", func(_ context.Context, body []byte) ([]byte, error) {
		var r map[string]string
		_ = json.Unmarshal(body, &r)
		reqs = append(reqs, r)
		return json.Marshal(reply)
	}); err != nil {
		t.Fatal(err)
	}
	return &reqs
}

func TestWebhookEvent_CreateEnqueuesFetch(t *testing.T) {
	h := newHarness(t, func(http.ResponseWriter, *http.Request) {})
	reqs := withLookup(t, h, map[string]string{"account_id": "acct", "user_id": "u1"})
	ev := webhookEvent(t, stravaEvent{ObjectType: "activity", ObjectID: 42, AspectType: "create", OwnerID: 9, SubscriptionID: 5})
	if err := handleWebhookEvent(context.Background(), h.w, ev); err != nil {
		t.Fatal(err)
	}
	jobs := h.fetchJobs(t)
	if len(jobs) != 1 || jobs[0].ExtID != "42" || jobs[0].AccountID != "acct" || jobs[0].Reason != "webhook" || !jobs[0].FetchStreams {
		t.Fatalf("jobs = %+v", jobs)
	}
	if r := (*reqs)[0]; r["external_id"] != "9" || r["subscription_id"] != "5" || r["provider"] != "strava" {
		t.Errorf("lookup request = %v", r)
	}
}

func TestWebhookEvent_DeletePublishesEvent(t *testing.T) {
	h := newHarness(t, func(http.ResponseWriter, *http.Request) {})
	reqs := withLookup(t, h, map[string]string{"account_id": "acct", "user_id": "u1"})
	ev := webhookEvent(t, stravaEvent{ObjectType: "activity", ObjectID: 42, AspectType: "delete", OwnerID: 9, EventTime: 1700000000})
	if err := handleWebhookEvent(context.Background(), h.w, ev); err != nil {
		t.Fatal(err)
	}
	got := h.published("cairn.events.source.deleted_upstream")
	if len(got) != 1 {
		t.Fatalf("deleted events = %d", len(got))
	}
	var body map[string]string
	_ = json.Unmarshal(got[0].Body, &body)
	if body["ext_id"] != "42" || body["event_time"] != "2023-11-14T22:13:20Z" {
		t.Errorf("event = %v", body)
	}
	if _, ok := (*reqs)[0]["subscription_id"]; ok {
		t.Error("zero subscription id must be omitted")
	}
}

func TestWebhookEvent_DropsAndRejects(t *testing.T) {
	h := newHarness(t, func(http.ResponseWriter, *http.Request) {})
	ctx := context.Background()
	var term *port.TerminalError

	if err := handleWebhookEvent(ctx, h.w, workersdk.WebhookEvent{Body: []byte("{")}); !errors.As(err, &term) || term.Reason != "bad_envelope" {
		t.Errorf("bad envelope = %v", err)
	}
	env, _ := json.Marshal(stravaWebhookEnvelope{Body: []byte("{")})
	if err := handleWebhookEvent(ctx, h.w, workersdk.WebhookEvent{Body: env}); !errors.As(err, &term) || term.Reason != "bad_event_payload" {
		t.Errorf("bad payload = %v", err)
	}
	if err := handleWebhookEvent(ctx, h.w, webhookEvent(t, stravaEvent{ObjectType: "athlete"})); err != nil {
		t.Errorf("athlete event = %v", err)
	}
	// No lookup responder: the orphaned event is dropped.
	if err := handleWebhookEvent(ctx, h.w, webhookEvent(t, stravaEvent{ObjectType: "activity", AspectType: "create"})); err != nil {
		t.Errorf("orphan = %v", err)
	}
	withLookup(t, h, map[string]string{"account_id": "acct"})
	if err := handleWebhookEvent(ctx, h.w, webhookEvent(t, stravaEvent{ObjectType: "activity", AspectType: "mystery"})); err != nil {
		t.Errorf("unknown aspect = %v", err)
	}
	if n := len(h.fetchJobs(t)); n != 0 {
		t.Errorf("enqueued %d jobs; want none", n)
	}
}

func TestLookupAccountByOwnerID_Errors(t *testing.T) {
	h := newHarness(t, func(http.ResponseWriter, *http.Request) {})
	withLookup(t, h, map[string]string{"error": "not_found"})
	if _, _, err := lookupAccountByOwnerID(context.Background(), h.w, "strava", "1", ""); err == nil || err.Error() != "not_found" {
		t.Errorf("error reply = %v", err)
	}
	_, _ = h.bus.RespondTo(context.Background(), "cairn.accounts.lookup_by_provider_ext", func(context.Context, []byte) ([]byte, error) {
		return []byte("{"), nil
	})
	if _, _, err := lookupAccountByOwnerID(context.Background(), h.w, "strava", "1", ""); err == nil {
		t.Error("bad reply: want error")
	}
}

func TestWebhookVerify(t *testing.T) {
	ctx := context.Background()
	verify := func(q map[string]string) workersdk.WebhookVerifyResponse {
		t.Helper()
		resp, err := handleWebhookVerify(ctx, nil, workersdk.WebhookVerifyRequest{Method: "GET", Query: q})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	t.Setenv("CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN", "")
	if r := verify(nil); r.Status != http.StatusServiceUnavailable {
		t.Errorf("unconfigured = %d", r.Status)
	}
	t.Setenv("CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN", "s3cret")
	if r := verify(map[string]string{"hub.mode": "unsubscribe"}); r.Status != http.StatusBadRequest {
		t.Errorf("bad mode = %d", r.Status)
	}
	if r := verify(map[string]string{"hub.mode": "subscribe", "hub.verify_token": "nope"}); r.Status != http.StatusForbidden {
		t.Errorf("bad token = %d", r.Status)
	}
	r := verify(map[string]string{"hub.mode": "subscribe", "hub.verify_token": "s3cret", "hub.challenge": "c42"})
	if r.Status != http.StatusOK || string(r.Body) != `{"hub.challenge":"c42"}` || r.ContentType != "application/json" {
		t.Errorf("ok = %+v", r)
	}
}
