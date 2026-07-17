// Package config carries the minimal NATS connection settings the worker's
// bus needs. This is a trimmed copy of cairn-core's config.NATSConfig — the
// worker only ever builds it as a plain struct literal (no envconfig loading),
// so the tags are retained for fidelity but carry no runtime dependency.
package config

import "time"

// NATSConfig is the NATS/JetStream connection + consumer tuning the bus uses.
type NATSConfig struct {
	URL string `envconfig:"URL" default:"nats://localhost:4222"`

	// ClusterName is the cluster the client expects to connect to. Empty
	// disables the check (useful for dev).
	ClusterName string `envconfig:"CLUSTER_NAME"`

	// ClientName is the connection's display name; workers override with
	// their own name.
	ClientName string `envconfig:"CLIENT_NAME" default:"cairn-server"`

	// Credentials: either CredsFile (NATS NKey/JWT) or User+Password.
	// Empty means anonymous, suitable only for dev. CredsFile wins.
	CredsFile string `envconfig:"CREDS_FILE"`
	Username  string `envconfig:"USERNAME"`
	Password  string `envconfig:"PASSWORD"`

	// TLS settings for connecting to NATS over wss/tls.
	TLSCAFile   string `envconfig:"TLS_CA_FILE"`
	TLSCertFile string `envconfig:"TLS_CERT_FILE"`
	TLSKeyFile  string `envconfig:"TLS_KEY_FILE"`

	// Connection tuning.
	ConnectTimeout time.Duration `envconfig:"CONNECT_TIMEOUT" default:"10s"`
	ReconnectWait  time.Duration `envconfig:"RECONNECT_WAIT" default:"2s"`
	MaxReconnects  int           `envconfig:"MAX_RECONNECTS" default:"-1"` // -1 = forever

	// JobAckWait is the JetStream ack window per job.
	JobAckWait time.Duration `envconfig:"JOB_ACK_WAIT" default:"5m"`

	// JobMaxDeliver caps how many times a failed job is retried before
	// JetStream parks it in the dead-letter consumer.
	JobMaxDeliver int `envconfig:"JOB_MAX_DELIVER" default:"5"`

	// StreamRetentionDays is how long completed-job records stay in the
	// results stream before eviction.
	StreamRetentionDays int `envconfig:"STREAM_RETENTION_DAYS" default:"30"`
}
