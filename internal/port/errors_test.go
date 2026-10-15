package port

import (
	"errors"
	"testing"
)

func TestErrorMessagesAndUnwrap(t *testing.T) {
	cause := errors.New("cause")
	naks := map[string]*NakWithDelayError{
		"r: cause":       {Reason: "r", Cause: cause},
		"r":              {Reason: "r"},
		"cause":          {Cause: cause},
		"nak-with-delay": {},
	}
	for want, e := range naks {
		if e.Error() != want {
			t.Errorf("nak %q = %q", want, e.Error())
		}
	}
	terms := map[string]*TerminalError{
		"r: cause":       {Reason: "r", Cause: cause},
		"r":              {Reason: "r"},
		"cause":          {Cause: cause},
		"terminal-error": {},
	}
	for want, e := range terms {
		if e.Error() != want {
			t.Errorf("term %q = %q", want, e.Error())
		}
	}
	if !errors.Is(&NakWithDelayError{Cause: cause}, cause) || !errors.Is(&TerminalError{Cause: cause}, cause) {
		t.Error("Unwrap must expose the cause")
	}
	if ErrKVKeyNotFound.Error() != "kv: key not found" {
		t.Error(ErrKVKeyNotFound.Error())
	}
}
