package queue

import "sync"

type Subscriber interface {
	// Subscribe must run in a goroutine before Recieve() is called
	// Subscribe needs a wait group to wait for the goroutine to finish
	// and you must call wg.Add(1) before calling Subscribe
	Subscribe(wg *sync.WaitGroup)

	Recieve() <-chan msg

	Error() <-chan error
}
