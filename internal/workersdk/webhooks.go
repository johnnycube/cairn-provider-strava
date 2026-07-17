package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// errAs is a one-line wrapper around errors.As. Used for compactness
// in the ack-disposition switch.
func errAs(err error, target any) bool { return errors.As(err, target) }

// ---------------------------------------------------------------------------
// Webhook helpers
//
// The cairn-server's HTTP layer is generic: POST /webhooks/<provider>
// forwards the raw body to cairn.webhooks.<provider>.event, GET
// /webhooks/<provider> request/replies to cairn.webhooks.<provider>.verify
// for handshake responses. The server has NO provider knowledge.
//
// Workers implement the provider-specific decoding here:
//
//	w.WebhookEvent(func(ctx, w, ev) error {
//	    // Decode Strava envelope, look up account, enqueue fetch_source.
//	})
//	w.WebhookVerify(func(ctx, w, req) (WebhookVerifyResponse, error) {
//	    // Validate verify_token, return challenge JSON.
//	})
//
// Each worker registers AT MOST one event handler and one verify handler
// — webhooks for a provider are conceptually a single endpoint.
// ---------------------------------------------------------------------------

// WebhookEvent is the message a worker receives for each POST to
// /webhooks/<provider>. Headers contain the original HTTP headers
// (lowercased keys) plus the X-Webhook-Source-Addr metadata the
// server-side handler added.
type WebhookEvent struct {
	Subject string
	Headers map[string]string
	Body    []byte

	// DeliveryAttempt mirrors the JobBus message's value — 1 on first
	// delivery, >1 on redelivery. Handlers can use this to detect
	// signature-validation failures that shouldn't be retried.
	DeliveryAttempt int
}

// WebhookEventHandler decodes a single POSTed webhook. Returning nil
// ACKs (worker processed it); returning *port.TerminalError Term()s
// (bad signature, malformed body — don't retry); any other error NAKs.
type WebhookEventHandler func(ctx context.Context, w *Worker, ev WebhookEvent) error

// WebhookVerifyRequest is the synchronous handshake the server sends
// when a GET /webhooks/<provider> request arrives. Query is the URL's
// parsed query parameters; Method is the HTTP method that triggered
// the request (always "GET" today, but kept open in case providers
// add other verb-based verifications).
type WebhookVerifyRequest struct {
	Method string
	Query  map[string]string
}

// WebhookVerifyResponse is what the worker sends back. The server
// writes the body verbatim with the chosen Status and Content-Type.
// Used e.g. for Strava's hub.challenge JSON echo.
type WebhookVerifyResponse struct {
	Status      int
	ContentType string
	Body        []byte
}

// WebhookVerifyHandler responds to a verify request. Returning an
// error makes the server return 500 to the HTTP caller; for graceful
// rejection (bad verify_token) return a Response with Status 403.
type WebhookVerifyHandler func(ctx context.Context, w *Worker, req WebhookVerifyRequest) (WebhookVerifyResponse, error)

// ---------------------------------------------------------------------------
// Registration + dispatch
// ---------------------------------------------------------------------------

// webhookHandlers carries the per-worker registered functions. Held on
// the Worker struct via a field that gets set in Run.
type webhookHandlers struct {
	mu     sync.Mutex
	event  WebhookEventHandler
	verify WebhookVerifyHandler
}

// WebhookEvent registers the event handler for this worker's provider.
// Must be called BEFORE Run. Calling it twice panics.
func (w *Worker) WebhookEvent(fn WebhookEventHandler) {
	w.webhooks.mu.Lock()
	defer w.webhooks.mu.Unlock()
	if w.webhooks.event != nil {
		panic("workersdk: WebhookEvent handler already registered")
	}
	w.webhooks.event = fn
}

// WebhookVerify registers the GET-handshake handler for this worker's
// provider. Must be called BEFORE Run. Calling it twice panics.
func (w *Worker) WebhookVerify(fn WebhookVerifyHandler) {
	w.webhooks.mu.Lock()
	defer w.webhooks.mu.Unlock()
	if w.webhooks.verify != nil {
		panic("workersdk: WebhookVerify handler already registered")
	}
	w.webhooks.verify = fn
}

// startWebhookSubscribers, invoked by Run, wires up the event-subscription
// and the verify-RespondTo if their handlers are registered. The
// returned slice closes alongside the rest of the worker's lifecycle.
func (w *Worker) startWebhookSubscribers(ctx context.Context) ([]port.Subscription, error) {
	w.webhooks.mu.Lock()
	eventFn := w.webhooks.event
	verifyFn := w.webhooks.verify
	w.webhooks.mu.Unlock()

	var subs []port.Subscription

	if eventFn != nil {
		subj := "cairn.webhooks." + w.WorkerKey() + ".event"
		ps, err := w.cfg.Bus.Pull(ctx, port.ConsumerConfig{
			Stream:        "CAIRN_WEBHOOKS",
			Durable:       w.cfg.Name + "__webhook_event",
			Subject:       subj,
			DeliverPolicy: port.DeliverAll,
		})
		if err != nil {
			return subs, fmt.Errorf("subscribe webhook events on %s: %w", subj, err)
		}
		subs = append(subs, ps)
		w.wg.Add(1)
		go w.webhookEventLoop(ctx, ps, eventFn)
	}

	if verifyFn != nil {
		subj := "cairn.webhooks." + w.WorkerKey() + ".verify"
		sub, err := w.cfg.Bus.RespondTo(ctx, subj, func(reqCtx context.Context, body []byte) ([]byte, error) {
			return w.dispatchVerify(reqCtx, verifyFn, body)
		})
		if err != nil {
			return subs, fmt.Errorf("respond_to webhook verify on %s: %w", subj, err)
		}
		subs = append(subs, sub)
	}

	return subs, nil
}

func (w *Worker) webhookEventLoop(
	ctx context.Context,
	sub port.PullSubscription,
	fn WebhookEventHandler,
) {
	defer w.wg.Done()
	log := w.logger.With("component", "webhook_events")

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}
		msgs, err := sub.Fetch(ctx, w.cfg.PullBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("fetch webhook event failed", "error", err)
			continue
		}
		for _, m := range msgs {
			msg := m.Message()
			ev := WebhookEvent{
				Subject:         msg.Subject,
				Headers:         msg.Headers,
				Body:            msg.Body,
				DeliveryAttempt: msg.DeliveryAttempt,
			}
			err := fn(ctx, w, ev)
			w.ackByError(ctx, m, "webhook event", err)
		}
	}
}

func (w *Worker) dispatchVerify(
	ctx context.Context,
	fn WebhookVerifyHandler,
	body []byte,
) ([]byte, error) {
	var req WebhookVerifyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("decode verify request: %w", err)
	}
	resp, err := fn(ctx, w, req)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("encode verify response: %w", err)
	}
	return out, nil
}

// ackByError centralises the message-disposition dance shared between
// the job dispatch loop and the webhook event loop.
func (w *Worker) ackByError(ctx context.Context, m port.PullMessage, kind string, err error) {
	if err == nil {
		if e := m.Ack(ctx); e != nil {
			w.logger.Warn("ack failed", "kind", kind, "error", e)
		}
		return
	}
	var term *port.TerminalError
	var nak *port.NakWithDelayError
	switch {
	case errAs(err, &term):
		if e := m.Term(ctx); e != nil {
			w.logger.Warn("term failed", "kind", kind, "error", e)
		}
	case errAs(err, &nak):
		if e := m.NakWithDelay(ctx, nak.Delay); e != nil {
			w.logger.Warn("nak-with-delay failed", "kind", kind, "error", e)
		}
	default:
		if e := m.Nak(ctx); e != nil {
			w.logger.Warn("nak failed", "kind", kind, "error", e)
		}
	}
}
