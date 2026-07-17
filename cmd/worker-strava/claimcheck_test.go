package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/johnnycube/cairn-provider-strava/internal/inmem"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
	workerv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/worker/v1"
)

func TestPublishResultViaBlob_UploadsBodyAndPublishesEnvelope(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()

	var uploaded []byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("blob upload method = %s; want PUT", r.Method)
		}
		uploaded, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	var presignReq workersdk.PresignUploadRequest
	if _, err := bus.RespondTo(ctx, "cairn.blobs.presign_upload.strava", func(_ context.Context, body []byte) ([]byte, error) {
		if err := json.Unmarshal(body, &presignReq); err != nil {
			return nil, err
		}
		return json.Marshal(workersdk.PresignedURL{Method: "PUT", URL: srv.URL, BlobID: "transfer/strava/b1.json"})
	}); err != nil {
		t.Fatal(err)
	}

	var published port.Message
	if _, err := bus.Subscribe(ctx, port.ConsumerConfig{Subject: "cairn.results.fetch_source.strava"}, func(_ context.Context, m port.Message) error {
		published = m
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	w, err := workersdk.New(workersdk.Config{Name: "strava-fetcher", Provider: "strava", Bus: bus})
	if err != nil {
		t.Fatal(err)
	}

	full := &workerv1.JobResult{
		WorkerName: "strava-fetcher",
		Events: []*workerv1.WorkerEvent{{
			Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_ACTIVITY,
			Entity: &workerv1.WorkerEvent_Activity{Activity: &workerv1.ImportedActivity{
				Ref: &workerv1.ExternalRef{Provider: "strava", ExternalId: "42"},
			}},
		}},
	}
	job := workersdk.Job{Subject: "cairn.jobs.fetch_source.strava"}
	if err := publishResultViaBlob(ctx, w, job, "result:j1:d1", full); err != nil {
		t.Fatalf("publishResultViaBlob: %v", err)
	}

	if presignReq.Kind != "result" {
		t.Errorf("presign kind = %q; want result", presignReq.Kind)
	}

	var stored workerv1.JobResult
	if err := protojson.Unmarshal(uploaded, &stored); err != nil {
		t.Fatalf("uploaded body is not a JobResult: %v", err)
	}
	if len(stored.GetEvents()) != 1 || stored.GetEvents()[0].GetActivity().GetRef().GetExternalId() != "42" {
		t.Fatalf("uploaded JobResult lost its events: %v", stored.GetEvents())
	}

	var env workerv1.JobResult
	if err := protojson.Unmarshal(published.Body, &env); err != nil {
		t.Fatalf("published envelope is not a JobResult: %v", err)
	}
	if len(env.GetEvents()) != 0 {
		t.Errorf("envelope must carry no events, got %d", len(env.GetEvents()))
	}
	ref := env.GetPayloadRef()
	if ref.GetBlobId() != "transfer/strava/b1.json" {
		t.Errorf("payload_ref.blob_id = %q", ref.GetBlobId())
	}
	sum := sha256.Sum256(uploaded)
	if ref.GetContentSha256() != hex.EncodeToString(sum[:]) {
		t.Errorf("payload_ref sha mismatch")
	}
	if ref.GetSizeBytes() != int64(len(uploaded)) {
		t.Errorf("payload_ref.size_bytes = %d; want %d", ref.GetSizeBytes(), len(uploaded))
	}
}

func TestPublishFailureResult_PublishesErrorEnvelope(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()

	var published port.Message
	if _, err := bus.Subscribe(ctx, port.ConsumerConfig{Subject: "cairn.results.fetch_source.strava"}, func(_ context.Context, m port.Message) error {
		published = m
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	w, err := workersdk.New(workersdk.Config{Name: "strava-fetcher", Provider: "strava", Bus: bus})
	if err != nil {
		t.Fatal(err)
	}

	in := fetchSourceJob{JobID: "j1", AccountID: "acc-1", UserID: "u-1", ExtID: "42"}
	job := workersdk.Job{Subject: "cairn.jobs.fetch_source.strava", DeliveryAttempt: 2}
	publishFailureResult(ctx, w, workerConfig{WorkerName: "strava-fetcher"}, job, in, "needs_reauth", nil)

	var env workerv1.JobResult
	if err := protojson.Unmarshal(published.Body, &env); err != nil {
		t.Fatalf("published failure is not a JobResult: %v", err)
	}
	if env.GetError().GetCode() != "needs_reauth" {
		t.Errorf("error code = %q", env.GetError().GetCode())
	}
	if env.GetError().GetClass() != workerv1.ErrorClass_ERROR_CLASS_AUTH_EXPIRED {
		t.Errorf("error class = %v", env.GetError().GetClass())
	}
	if env.GetFailedRef().GetExternalId() != "42" || env.GetFailedRef().GetExternalAccountId() != "acc-1" {
		t.Errorf("failed_ref = %v", env.GetFailedRef())
	}
}
