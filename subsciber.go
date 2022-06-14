package queue

type Subscriber interface {
	Subscibe() <-chan Msg
}
