// Package config carries the minimal NATS connection settings the worker's
// bus needs. This is a trimmed copy of cairn-core's config.NATSConfig — the
// worker only ever builds it as a plain struct literal (no envconfig loading),
// so the tags are retained for fidelity but carry no runtime dependency.
package config

import "time"

// NATSConfig is the NATS/JetStream connection + consumer tuning the bus uses.
type NATSConfig struct {
	URL string `envconfig:"URL" default:"nats://localhost:4222"`

	// ClientName is the connection's display name; workers override with
	// their own name.
	ClientName string `envconfig:"CLIENT_NAME" default:"cairn-server"`

	// JobAckWait is the JetStream ack window per job.
	JobAckWait time.Duration `envconfig:"JOB_ACK_WAIT" default:"5m"`

	// JobMaxDeliver caps how many times a failed job is retried before
	// JetStream parks it in the dead-letter consumer.
	JobMaxDeliver int `envconfig:"JOB_MAX_DELIVER" default:"5"`
}
