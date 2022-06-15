package queue

import (
	"encoding/json"
	"math/rand"
	"time"
)

// union is a message that can contain multiple messages
// union also implements the sarama.Encoder interface
type union struct {
	ID        string            `json:"id"`
	Timestamp int64             `json:"timestamp"`
	Messages  map[msgType][]msg `json:"messages"`
}

func newUnion() *union {
	u := &union{
		Timestamp: time.Now().Unix(),
		Messages:  make(map[msgType][]msg),
	}

	return u.generateId()
}

func (u *union) Encode() ([]byte, error) {
	b, err := json.Marshal(u)
	return b, err
}

func (u *union) Length() int {
	b, _ := u.Encode()
	return len(b)
}

// decode a union from a byte array
func (u *union) decode(b []byte) error {
	err := json.Unmarshal(b, u)
	return err
}

// generate a random union id with union_ prefix
func (u *union) generateId() *union {
	u.ID = "union_" + generateRandomString(10)
	return u

}

// implement json.Unmarshaler interface
func (u *union) UnmarshalJSON(b []byte) error {
	var tmpUnion struct {
		ID        string                    `json:"id"`
		Timestamp int64                     `json:"timestamp"`
		Messages  map[msgType][]interface{} `json:"messages"`
	}

	err := json.Unmarshal(b, &tmpUnion)
	if err != nil {
		return err
	}

	for k := range tmpUnion.Messages {
		switch k {
		case createCustomerMsgType:
			var result struct {
				ID        string                           `json:"id"`
				Timestamp int64                            `json:"timestamp"`
				Messages  map[msgType][]*createCustomerMsg `json:"messages"`
			}
			err = json.Unmarshal(b, &result)
			if err != nil {
				return err
			}
			u.ID = result.ID
			u.Timestamp = result.Timestamp

			for _, m := range result.Messages[k] {
				u.Messages[k] = append(u.Messages[k], m)
			}
		default:
			// retrun an error if the message type is not supported
			return errUnsupportedMessageType
		}
	}
	return nil
}

// generate random string from a-z and A-Z and 0-9
func generateRandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
