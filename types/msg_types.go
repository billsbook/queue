package types

type MsgType string

const (
	CustomerCreatedMT MsgType = "customer.created"
	CustomerDeletedMT MsgType = "customer.deleted"
	CustomerUpdatedMT MsgType = "customer.updated"
)
