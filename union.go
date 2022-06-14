package queue

import (
	"encoding/json"
	"math/rand"
	"time"
)

// union is a message that can contain multiple messages
// union also implements the sarama.Encoder interface
type union struct {
	ID        string
	Timestamp int64
	Messages  map[msgType]map[int]Msg
}

func newUnion() *union {
	u := &union{
		Timestamp: time.Now().Unix(),
		Messages:  make(map[msgType]map[int]Msg),
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
func (u *union) Decode(b []byte) error {
	err := json.Unmarshal(b, u)
	return err
}

// generate a random union id with union_ prefix
func (u *union) generateId() *union {
	u.ID = "union_" + generateRandomString(10)
	return u

}

// generate random string from a-z and A-Z and 0-9
func generateRandomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
