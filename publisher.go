package queue

type Publisher interface {
	Publish(msgs map[int]Msg) error
}
