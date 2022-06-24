package queue

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v9"

	"github.com/billsbook/queue/models"
	"github.com/billsbook/queue/types"
)

// pubStore is an implementation of PublisherStore which connect to an redis server
type pubStore struct {
	client *redis.Client
	ctx    context.Context
	key    string
}

func newPubStore(service Service) types.PublisherStore {
	s := &pubStore{
		client: redis.NewClient(&redis.Options{
			Addr: "localhost:6379"}),
		ctx: context.Background(),
		key: fmt.Sprintf("%s:%s", service, "failed_to_publish"),
	}

	// ping redis
	_, err := s.client.Ping(s.ctx).Result()
	if err != nil {
		panic(err)
	}

	return s
}

func (s *pubStore) SaveMsg(msgs ...types.Msg) error {

	mi := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		mi[i] = msg
	}

	r := s.client.RPush(s.ctx, s.key, mi)
	return r.Err()
}

func (s *pubStore) RetrieveMsg() (types.Msg, error) {
	r := s.client.LIndex(s.ctx, s.key, 0)
	b, err := r.Bytes()
	if err != nil {
		return nil, err
	}
	return models.DecodeMsg(b)
}

func (s *pubStore) Remove(msg types.Msg) error {
	r := s.client.LRem(s.ctx, s.key, 0, msg)
	return r.Err()
}
