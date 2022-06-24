package queue

import "github.com/billsbook/queue/types"

type Publisher interface {
	Publish(msg types.Msg) error
}
