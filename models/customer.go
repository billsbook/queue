package models

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/billsbook/common"
	"github.com/billsbook/queue/types"
)

type Customer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type customerCreatedMsg struct {
	Id        string        `json:"id"`
	MsgType   types.MsgType `json:"message_type"`
	Timestamp int64         `json:"timestamp"`
	Customer  Customer      `json:"customer"`
}

func (m *customerCreatedMsg) ID() string {
	return m.Id
}

func (m *customerCreatedMsg) MessageType() types.MsgType {
	return m.MsgType
}

// Target method define Topic
func (m *customerCreatedMsg) Target() string {
	return string(types.CustomerTopic)
}

func (m *customerCreatedMsg) Value() interface{} {
	return m.Customer
}

func (m *customerCreatedMsg) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *customerCreatedMsg) Length() int {
	b, _ := m.Encode()
	return len(b)
}

func (m *customerCreatedMsg) MarshalBinary() ([]byte, error) {
	return m.Encode()

}

func CreateCustomerMsg(id, name, email string) types.Msg {
	m := &customerCreatedMsg{
		Id:        common.GenRandString(5),
		MsgType:   types.CustomerCreatedMT,
		Timestamp: time.Now().Unix(),
		Customer: Customer{
			ID:    id,
			Name:  name,
			Email: email,
		},
	}

	return m
}

type customerUpdatedMsg struct {
	Id        string        `json:"id"`
	MsgType   types.MsgType `json:"message_type"`
	Timestamp int64         `json:"timestamp"`
	Customer  Customer      `json:"customer"`

	// PreviousAttributes is used to track the previous attributes of the customer
	// this is used to track the differences between the previous and current attributes
	// of the customer
	PreviousAttributes map[string]interface{} `json:"previous_attributes"`
}

func (m *customerUpdatedMsg) ID() string {
	return m.Id
}

func (m *customerUpdatedMsg) MessageType() types.MsgType {
	return m.MsgType
}

func (m *customerUpdatedMsg) Target() string {
	return string(types.CustomerTopic)
}

func (m *customerUpdatedMsg) Value() interface{} {
	return m.Customer
}

func (m *customerUpdatedMsg) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *customerUpdatedMsg) Length() int {
	b, _ := m.Encode()
	return len(b)
}

func (m *customerUpdatedMsg) MarshalBinary() ([]byte, error) {
	return m.Encode()

}

func UpdateCustomerMsg(customer *Customer, previousAttributes map[string]interface{}) types.Msg {
	m := &customerUpdatedMsg{
		Id:                 common.GenRandString(5),
		MsgType:            types.CustomerUpdatedMT,
		Timestamp:          time.Now().Unix(),
		Customer:           *customer,
		PreviousAttributes: previousAttributes,
	}

	return m
}

type customerDeletedMsg struct {
	Id         string        `json:"id"`
	MsgType    types.MsgType `json:"message_type"`
	Timestamp  int64         `json:"timestamp"`
	CustomerId string        `json:"customer_id"`
}

func (m *customerDeletedMsg) ID() string {
	return m.Id
}

func (m *customerDeletedMsg) MessageType() types.MsgType {
	return m.MsgType
}

func (m *customerDeletedMsg) Target() string {
	return string(types.CustomerTopic)
}

func (m *customerDeletedMsg) Value() interface{} {
	return m.CustomerId
}

func (m *customerDeletedMsg) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *customerDeletedMsg) Length() int {
	b, _ := m.Encode()
	return len(b)
}

func (m *customerDeletedMsg) MarshalBinary() ([]byte, error) {
	return m.Encode()
}

func DeleteCustomerMsg(customerId string) types.Msg {
	m := &customerDeletedMsg{
		Id:         common.GenRandString(5),
		MsgType:    types.CustomerDeletedMT,
		Timestamp:  time.Now().Unix(),
		CustomerId: customerId,
	}

	return m
}

func CustomerDecode(b []byte, t string) (types.Msg, error) {

	switch t {
	case "created":
		m := &customerCreatedMsg{}
		err := json.Unmarshal(b, m)
		return m, err
	case "deleted":
		m := &customerDeletedMsg{}
		err := json.Unmarshal(b, m)
		return m, err
	case "updated":
		m := &customerUpdatedMsg{}
		err := json.Unmarshal(b, m)
		return m, err
	default:
		return nil, errors.New("invalid message type")

	}
}
