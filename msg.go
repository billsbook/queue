package queue

type msgType int

const (
	createCustomerMsgType msgType = iota
)

// msg is the interface that all messages that are sent to the message broker must implement
type msg interface {
	// MsgType returns the type of the message
	msgType() msgType

	// Target returns the topic that the message should be sent to
	target() string

	Value() interface{}
}
