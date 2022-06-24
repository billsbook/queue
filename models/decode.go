package models

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/billsbook/queue/types"
)

func DecodeMsg(b []byte) (types.Msg, error) {

	res := make(map[string]interface{})
	err := json.Unmarshal(b, &res)
	if err != nil {
		return nil, err
	}

	msgType := res["message_type"].(string)
	ms := strings.Split(msgType, ".")
	switch ms[0] {
	case "customer":
		return CustomerDecode(b, ms[1])

	default:
		return nil, errors.New("invalid message type")
	}

}
