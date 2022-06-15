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
	Name  string `json:"name"`
	Email string `json:"email"`
}

type createCustomerMsg struct {
	ID        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Customer  customer `json:"customer"`
}

// Target method define Topic
func (c *createCustomerMsg) target() string {
	return "costumer"
}

func (m *createCustomerMsg) msgType() msgType {
	return createCustomerMsgType
}

func (m *createCustomerMsg) Value() interface{} {
	return m.Customer
}

func CreateConsumerMsg(name, email string) msg {
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
