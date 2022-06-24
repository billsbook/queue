package types

// PublisherStore is an interface for a persistent storage of failed events.
// It should save and retrieve messages with a FIFO pattern.
// each service should have its own implementation of this interface
type PublisherStore interface {
	// SaveMsg method for saving an failed message to a persistent storage
	SaveMsg(msgs ...Msg) error

	// Retrieve method for retrieving failed message from a persistent storage
	RetrieveMsg() (Msg, error)

	// Rmove method signals that a message has been processed
	// and should be removed from the persistent storage
	Remove(msg Msg) error
}

type SubscriberStore interface {

	// SaveMsgID method for saving a processed message to a persistent storage
	SaveMsgID(msg Msg) error

	// IsProcessed method for checking if a message has been processed before.
	IsProcessed(msg Msg) bool
}
