package queue

import (
	"sync"

	"github.com/billsbook/queue/types"
)

type Subscriber interface {
	// Subscribe must run in a goroutine before Recieve() is called
	// Subscribe needs a wait group to wait for the goroutine to finish
	// and you must call wg.Add(1) before calling Subscribe
	Subscribe(wg *sync.WaitGroup)

	Recieve() <-chan types.Msg

	// Processed must be called when the recieved message is processed
	Processed(msg types.Msg) error

	Error() <-chan error
}
