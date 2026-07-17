package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
	workerv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/worker/v1"
)

// photoFetchSize is the max pixel dimension requested from Strava's photos
// endpoint — large enough for a detail-page gallery without pulling originals.
const photoFetchSize = 2048

// fetchActivityAttachments mirrors an activity's photos into blob storage and
// returns them as Attachments. Strava photo URLs are short-lived CDN links, so
// we download the bytes and re-upload them to Cairn's store.
//
// It pulls the full photo list (GET /activities/{id}/photos, one API call); if
// that's unavailable (rate-limited, error, empty) it falls back to the primary
// photo already present on the activity detail — so a manual import without
// budget still gets the cover photo. Best-effort throughout: any failure yields
// fewer/no attachments, never a failed import.
func fetchActivityAttachments(ctx context.Context, w *workersdk.Worker, client *stravaClient, accessToken, bucket, userID, activityID string, act *stravaActivity) []*workerv1.Attachment {
	photos := listActivityPhotos(ctx, w, client, accessToken, bucket, activityID, act)
	if len(photos) == 0 {
		return nil
	}
	out := make([]*workerv1.Attachment, 0, len(photos))
	for _, p := range photos {
		url, width, height := selectPhoto(p.Urls, p.Sizes)
		if url == "" {
			continue
		}
		att := mirrorPhoto(ctx, w, userID, activityID, url)
		if att == nil {
			continue
		}
		att.Caption = p.Caption
		att.Width = int32(width)
		att.Height = int32(height)
		out = append(out, att)
	}
	return out
}

// listActivityPhotos returns the activity's photos, preferring the full list
// endpoint and falling back to the primary photo on the activity detail.
func listActivityPhotos(ctx context.Context, w *workersdk.Worker, client *stravaClient, accessToken, bucket, activityID string, act *stravaActivity) []stravaPhoto {
	if err := reserveRead(ctx, w, bucket); err == nil {
		if photos, err := client.GetActivityPhotos(ctx, accessToken, activityID, photoFetchSize); err == nil && len(photos) > 0 {
			return photos
		} else if err != nil {
			w.Logger().Warn("photo list fetch failed; falling back to primary", "activity", activityID, "error", err)
		}
	}
	// Fallback: the primary photo embedded in the activity detail (no API call).
	if act != nil && act.Photos.Primary != nil && len(act.Photos.Primary.Urls) > 0 {
		return []stravaPhoto{{
			UniqueID: act.Photos.Primary.UniqueID,
			Urls:     act.Photos.Primary.Urls,
		}}
	}
	return nil
}

// mirrorPhoto downloads the photo bytes and uploads them to blob storage,
// returning an Attachment with blob_id/external_url/content_type set (caption
// and dimensions are filled by the caller). nil on any failure.
func mirrorPhoto(ctx context.Context, w *workersdk.Worker, userID, activityID, url string) *workerv1.Attachment {
	data, ct, err := downloadPhoto(ctx, url)
	if err != nil {
		w.Logger().Warn("photo download failed", "activity", activityID, "error", err)
		return nil
	}
	up, err := w.PresignUpload(ctx, workersdk.PresignUploadRequest{
		UserID:        userID, // keys the blob under users/<id>/ for per-user deletion
		ContentType:   ct,
		ContentLength: int64(len(data)),
	})
	if err != nil {
		w.Logger().Warn("photo presign failed", "activity", activityID, "error", err)
		return nil
	}
	method := up.Method
	if method == "" {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, up.URL, bytes.NewReader(data))
	if err != nil {
		return nil
	}
	for k, v := range up.RequiredHeaders {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", ct)
	}
	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		w.Logger().Warn("photo upload failed", "activity", activityID, "error", err)
		return nil
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		w.Logger().Warn("photo upload status", "activity", activityID, "status", resp.StatusCode)
		return nil
	}
	return &workerv1.Attachment{
		BlobId:      up.BlobID,
		ExternalUrl: url,
		ContentType: ct,
	}
}

// selectPhoto picks the URL with the biggest numeric size key and returns its
// [width, height] from the sizes map (0,0 when unknown). Strava keys both maps
// by pixel size as a string (e.g. "256", "2048").
func selectPhoto(urls map[string]string, sizes map[string][]int) (url string, width, height int) {
	bestKey, bestSize := "", -1
	for size, u := range urls {
		if u == "" {
			continue
		}
		n, err := strconv.Atoi(size)
		if err != nil {
			if url == "" { // non-numeric key fallback
				url, bestKey = u, size
			}
			continue
		}
		if n > bestSize {
			url, bestKey, bestSize = u, size, n
		}
	}
	if dims, ok := sizes[bestKey]; ok && len(dims) == 2 {
		width, height = dims[0], dims[1]
	}
	return url, width, height
}

func downloadPhoto(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("photo GET status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // 32 MiB cap
	if err != nil {
		return nil, "", err
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = http.DetectContentType(data)
	}
	return data, ct, nil
}
