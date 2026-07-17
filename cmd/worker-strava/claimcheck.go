package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
	workerv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/worker/v1"
)

// publishResultViaBlob claim-checks an event-carrying JobResult: the full
// protojson body is PUT to the blob store via a server-minted presigned URL
// (kind "result", short-lived — the server deletes it after ingest, a bucket
// lifecycle rule reaps orphans), and only a small envelope with payload_ref
// travels over NATS. Results of any size stay publishable — big activities
// used to exceed the broker's max_payload and Term as result_too_large.
//
// Presign/upload failures are transient (NAK): MinIO being briefly
// unreachable must not fail the activity.
func publishResultViaBlob(ctx context.Context, w *workersdk.Worker, job workersdk.Job, msgID string, jr *workerv1.JobResult) error {
	body, err := protojson.Marshal(jr)
	if err != nil {
		return &port.TerminalError{Reason: "marshal_result", Cause: err}
	}
	sum := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sum[:])

	up, err := w.PresignUpload(ctx, workersdk.PresignUploadRequest{
		Kind:          "result",
		ContentType:   "application/json",
		ContentLength: int64(len(body)),
		ContentSHA256: shaHex,
	})
	if err != nil {
		return fmt.Errorf("presign result upload: %w", err)
	}
	if err := putPresigned(ctx, up, body, "application/json"); err != nil {
		return fmt.Errorf("upload result payload: %w", err)
	}

	envelope := &workerv1.JobResult{
		WorkerName:    jr.GetWorkerName(),
		WorkerVersion: jr.GetWorkerVersion(),
		WorkerPackage: jr.GetWorkerPackage(),
		NewWatermark:  jr.GetNewWatermark(),
		MoreAvailable: jr.GetMoreAvailable(),
		PayloadRef: &workerv1.PayloadRef{
			BlobId:        up.BlobID,
			SizeBytes:     int64(len(body)),
			ContentType:   "application/json",
			ContentSha256: shaHex,
		},
	}
	envBody, err := protojson.Marshal(envelope)
	if err != nil {
		return &port.TerminalError{Reason: "marshal_result", Cause: err}
	}
	return w.PublishResult(ctx, job, msgID, envBody)
}

// publishFailureResult tells the server a job failed terminally, so the
// import-queue item is failed NOW with the true reason. Without it a Term'd
// job publishes nothing and the item sits in_progress until the stale reaper
// masks the cause as "no result after N dispatch attempts". Best-effort —
// the Term disposition that follows is authoritative for the job itself.
func publishFailureResult(ctx context.Context, w *workersdk.Worker, cfg workerConfig, job workersdk.Job, in fetchSourceJob, reason string, cause error) {
	if in.ExtID == "" {
		return // nothing to correlate the failure to
	}
	class := workerv1.ErrorClass_ERROR_CLASS_INVALID_INPUT
	switch reason {
	case "needs_reauth":
		class = workerv1.ErrorClass_ERROR_CLASS_AUTH_EXPIRED
	case "not_found":
		class = workerv1.ErrorClass_ERROR_CLASS_NOT_FOUND
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	body, err := protojson.Marshal(&workerv1.JobResult{
		WorkerName:    cfg.WorkerName,
		WorkerVersion: workerVersion,
		WorkerPackage: workerPackage,
		Error: &workerv1.WorkerError{
			Class:   class,
			Code:    reason,
			Message: msg,
		},
		FailedRef: &workerv1.ExternalRef{
			UserId:            in.UserID,
			Provider:          workerProvider,
			ExternalAccountId: in.AccountID,
			ExternalId:        in.ExtID,
		},
	})
	if err != nil {
		return
	}
	msgID := fmt.Sprintf("failed:%s:%s:d%d", workerProvider, in.ExtID, job.DeliveryAttempt)
	if perr := w.PublishResult(ctx, job, msgID, body); perr != nil {
		w.Logger().Warn("failure result publish failed", "ext_id", in.ExtID, "reason", reason, "error", perr)
	}
}
