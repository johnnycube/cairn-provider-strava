package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

// archivedActivity is the self-describing blob format the worker stores in
// object storage on import and re-reads in parse_blob. Holding both the raw
// activity and its streams means a re-parse can reconstruct the full payload
// (summary + stream) without any provider API call.
//
// The Provenance block records exactly which worker/version/package/manifest
// wrote the blob, so a later re-parse can tell whether the current worker logic
// differs from the one that produced it (and is therefore worth re-running).
// This metadata is embedded IN the blob — independent of the activity_sources
// row — so the archive stays self-describing even if the DB row is lost or the
// blob is inspected out of band.
type archivedActivity struct {
	Version    int                   `json:"version"`
	Provenance archiveProvenance     `json:"provenance"`
	Activity   *stravaActivity       `json:"activity"`
	Streams    stravaStreamsResponse `json:"streams,omitempty"`
}

type archiveProvenance struct {
	Provider      string    `json:"provider"`
	WorkerName    string    `json:"worker_name"`
	WorkerVersion string    `json:"worker_version"`
	Package       string    `json:"package"`
	ArchivedAt    time.Time `json:"archived_at"`
}

const archivedActivityVersion = 1

var archiveHTTPClient = &http.Client{Timeout: 30 * time.Second}

// archiveActivityBlob marshals the raw fetched activity (+streams) plus worker
// provenance and uploads it via a server-minted presigned PUT, returning the
// blob id (full S3 key) to store as raw_blob_id and the archived byte size (the
// content type is always application/json). Best-effort: returns an error on
// any failure; the caller logs and continues, since the import is durable
// without the archive.
func archiveActivityBlob(ctx context.Context, w *workersdk.Worker, cfg workerConfig, userID string, act *stravaActivity, streams stravaStreamsResponse) (blobID string, sizeBytes int64, err error) {
	body, err := json.Marshal(archivedActivity{
		Version: archivedActivityVersion,
		Provenance: archiveProvenance{
			Provider:      workerProvider,
			WorkerName:    cfg.WorkerName,
			WorkerVersion: workerVersion,
			Package:       workerPackage,
			ArchivedAt:    time.Now().UTC(),
		},
		Activity: act,
		Streams:  streams,
	})
	if err != nil {
		return "", 0, fmt.Errorf("marshal archive: %w", err)
	}

	up, err := w.PresignUpload(ctx, workersdk.PresignUploadRequest{
		UserID:        userID,
		ContentType:   "application/json",
		ContentLength: int64(len(body)),
	})
	if err != nil {
		return "", 0, fmt.Errorf("presign upload: %w", err)
	}
	if err := putPresigned(ctx, up, body, "application/json"); err != nil {
		return "", 0, err
	}
	return up.BlobID, int64(len(body)), nil
}

// putPresigned uploads body to a server-minted presigned URL, sending the
// required headers verbatim. Shared by the raw-archive and claim-check
// result paths.
func putPresigned(ctx context.Context, up workersdk.PresignedURL, body []byte, contentType string) error {
	method := up.Method
	if method == "" {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, up.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range up.RequiredHeaders {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("blob PUT: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("blob PUT status %d", resp.StatusCode)
	}
	return nil
}

// downloadArchivedActivity GETs and decodes an archived blob from a presigned
// URL (the parse_blob read path).
func downloadArchivedActivity(ctx context.Context, url string) (archivedActivity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return archivedActivity{}, err
	}
	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		return archivedActivity{}, fmt.Errorf("blob GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return archivedActivity{}, fmt.Errorf("blob GET status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // 64 MiB cap
	if err != nil {
		return archivedActivity{}, err
	}
	var out archivedActivity
	if err := json.Unmarshal(data, &out); err != nil {
		return archivedActivity{}, fmt.Errorf("decode archive: %w", err)
	}
	if out.Activity == nil {
		return archivedActivity{}, fmt.Errorf("archive has no activity")
	}
	return out, nil
}
