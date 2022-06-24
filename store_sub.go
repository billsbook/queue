package queue

import (
	"context"
	"fmt"

	"github.com/billsbook/queue/types"
	"github.com/go-redis/redis/v9"
)

type subStore struct {
	client *redis.Client
	ctx    context.Context
	key    string
}

func newSubStore(service Service) types.SubscriberStore {
	return &subStore{
		client: redis.NewClient(&redis.Options{
			Addr: "localhost:6379"}),
		ctx: context.Background(),
		key: fmt.Sprintf("%s:%s", service, "proccessed_messages"),
	}
}

func (s *subStore) SaveMsgID(msg types.Msg) error {
	return s.client.SAdd(s.ctx, fmt.Sprintf("%s:%s", s.key, msg.MessageType()), msg.ID()).Err()
}

func (s *subStore) IsProcessed(msg types.Msg) bool {
	return s.client.SIsMember(s.ctx, fmt.Sprintf("%s:%s", s.key, msg.MessageType()), msg.ID()).Val()
}
