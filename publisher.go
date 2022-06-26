package queue

import (
	"sync"

	"github.com/billsbook/queue/types"
)

type Publisher interface {
	Publish(msg types.Msg) error

	// RetryEngine starts a retry engine that will retry publishing failed messages
	// must be runed in a separate goroutine
	RetryEngine(wg *sync.WaitGroup)
}
