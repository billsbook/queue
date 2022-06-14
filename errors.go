package queue

import "errors"

// ErrTargetMismatch represents an error when the target of a message does not match the target of other messages
var ErrTargetMismatch = errors.New("target mismatch")
