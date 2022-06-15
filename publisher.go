package queue

type Publisher interface {
	Publish(msgs ...msg) error
}
