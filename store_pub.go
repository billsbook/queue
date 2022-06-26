package queue

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/go-redis/redis/v9"

	"github.com/billsbook/queue/models"
	"github.com/billsbook/queue/types"
)

type retryEngine struct {
	maxRetries int
	attempts   int
	isActive   int64 // atomic bool
	activate   chan struct{}
}

// pubStore handle failed messages that could not be published
// save them in a redis list and retry later base on a retry policy
type pubStore struct {
	client *redis.Client
	ctx    context.Context
	key    string
	engine *retryEngine
}

func newPubStore(service Service) *pubStore {
	s := &pubStore{
		client: redis.NewClient(&redis.Options{
			Addr: "localhost:6379"}),
		ctx: context.Background(),
		key: fmt.Sprintf("%s:%s", service, "failed_to_publish"),
		engine: &retryEngine{
			maxRetries: 10,
			attempts:   0,
			isActive:   0,
			activate:   make(chan struct{}),
		},
	}

	// ping redis
	if err := s.client.Ping(s.ctx).Err(); err != nil {
		panic(err)
	}
	return s
}

func (s *pubStore) saveMsg(msgs ...types.Msg) error {

	mi := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		mi[i] = msg
	}

	r := s.client.RPush(s.ctx, s.key, mi)
	if r.Err() != nil {
		return r.Err()
	}

	if atomic.LoadInt64(&s.engine.isActive) == 0 {
		s.engine.activate <- struct{}{}
	}

	return nil
}

func (s *pubStore) retrieveMsg() (types.Msg, error) {
	r := s.client.LIndex(s.ctx, s.key, 0)
	b, err := r.Bytes()
	if err != nil {
		return nil, err
	}
	return models.DecodeMsg(b)
}

func (s *pubStore) remove(msg types.Msg) error {
	r := s.client.LRem(s.ctx, s.key, 0, msg)
	return r.Err()
}
