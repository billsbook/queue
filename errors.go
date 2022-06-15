package queue

import "errors"

// errTargetMismatch represents an error when the target of a message does not match the target of other messages
var errTargetMismatch = errors.New("target mismatch")
var errUnsupportedMessageType = errors.New("unsupported message type")
