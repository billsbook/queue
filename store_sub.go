package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/billsbook/queue/types"
	"github.com/go-redis/redis/v9"
)

type subStore struct {
	client     *redis.Client
	ctx        context.Context
	key        string
	ticker     *time.Ticker
	windowSize time.Duration
}

func newSubStore(service Service) *subStore {
	s := &subStore{
		client: redis.NewClient(&redis.Options{
			Addr: "localhost:6379"}),
		ctx:        context.Background(),
		key:        fmt.Sprintf("%s:%s", service, "proccessed_messages"),
		windowSize: 5,
	}
	s.ticker = time.NewTicker(time.Minute * s.windowSize)
	return s
}

func (s *subStore) SaveMsgID(msg types.Msg) error {
	return s.client.ZAdd(s.ctx, fmt.Sprintf("%s:%s", s.key, msg.MessageType()), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: msg.ID(),
	}).Err()
}

func (s *subStore) IsProcessed(msg types.Msg) bool {
	return s.client.ZScore(s.ctx, fmt.Sprintf("%s:%s", s.key, msg.MessageType()), msg.ID()).Err() != redis.Nil
}

// use subStore.ticker to clean up the processed messages
func (s *subStore) Clean() {
	for range s.ticker.C {

		// retrieve keys with the pattern "service:proccessed_messages:*"
		keys, err := s.client.Keys(s.ctx, fmt.Sprintf("%s:*", s.key)).Result()
		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, key := range keys {

			fmt.Printf("cleaning up %s members which are older than %d minute\n", key, s.windowSize)

			err = s.client.ZRemRangeByScore(s.ctx, key, "-inf",
				fmt.Sprintf("%v", time.Now().Add(-s.windowSize*time.Minute).Unix())).Err()

			if err != nil {
				fmt.Println(err)
				continue
			}
		}

	}
}
