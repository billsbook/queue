package types

import (
	"encoding"

	"github.com/Shopify/sarama"
)

// Msg is the interface that all messages that are sent to the message broker must implement
// Note: second paramether of Msg must be `messeage_type` in the message
// `message_type` is a string in the format of `module.operation`
// `module` is the module that the message belongs to
// `operation` is the type of operation that the message represents
// example: `customer.deleted`
type Msg interface {
	ID() string
	// ModelType
	MessageType() MsgType

	// Target returns the topic that the message should be sent to
	Target() string

	Value() interface{}

	// encoder interface for encoding the message to a byte array
	encoder
}

type encoder interface {
	encoding.BinaryMarshaler
	sarama.Encoder
}
