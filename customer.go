package queue

import (
	"strconv"
	"sync/atomic"
	"time"
)

var (

	// createCustomerCount is used to generate sequential customer IDs
	// TODO: implement a mechanism that retrive this number from a persistent store
	createCustomerCount uint64 = 0
)

type customer struct {
	Name  string
	Email string
}

type createCustomerMsg struct {
	ID        string
	Timestamp int64
	Customer  customer
}

// Target method define Topic
func (c *createCustomerMsg) Target() string {
	return "Customers"
}

func (m *createCustomerMsg) MsgType() msgType {
	return createCustomerMsgType
}

func CreateConsumerMsg(name, email string) Msg {
	m := &createCustomerMsg{
		ID:        "id_" + strconv.Itoa(int(atomic.LoadUint64(&createCustomerCount))),
		Timestamp: time.Now().Unix(),
		Customer: customer{
			Name:  name,
			Email: email,
		},
	}

	atomic.AddUint64(&createCustomerCount, 1)
	return m
}
