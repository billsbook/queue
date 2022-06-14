package queue

type msgType int

const (
	createCustomerMsgType msgType = iota
)

// Msg is the interface that all messages that are sent to the message broker must implement
type Msg interface {
	// MsgType returns the type of the message
	MsgType() msgType

	// Target returns the topic that the message should be sent to
	Target() string
}
