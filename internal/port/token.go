package port

import "time"

// errInternal is a tiny dependency-free error helper used by sentinel
// errors in this package (e.g. ErrKVKeyNotFound in job_bus.go).
type errInternal string

func (e errInternal) Error() string { return string(e) }

// TokenState is a provider OAuth token as exchanged over the NATS token bus.
// Trimmed copy of cairn-core's port.TokenState (the repo interfaces that
// reference domain types are server-side and not needed by the worker).
type TokenState struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	Scope        string
	TokenType    string // typically "Bearer"
}
