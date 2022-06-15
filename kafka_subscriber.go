package queue

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
)

type KafkaGroupID string

const (
	CostumerGroup KafkaGroupID = "customer_service"
)

// kafkaSubscriber is respoinsible for subscribing to multiple topics
//  in a cluster of consumer groups
// also implements the sarama.ConsumerGroupHandler interface
type kafkaSubscriber struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string

	groupHandler groupHandler

	errors chan error
}

// NewKafkaSubscriber creates a new kafka subscriber
func NewKafkaSubscriber(group KafkaGroupID, brokers, topics []string) (Subscriber, error) {
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		return nil, err
	}

	g, err := sarama.NewConsumerGroupFromClient(string(group), client)
	if err != nil {
		return nil, err
	}

	return &kafkaSubscriber{
		consumerGroup: g,
		topics:        topics,
		groupHandler: groupHandler{
			reciever: make(chan msg),
		}}, nil
}

func (s *kafkaSubscriber) Recieve() <-chan msg {
	return s.groupHandler.reciever
}

// Subscribe must run in a goroutine
func (s *kafkaSubscriber) Subscribe(wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()

	for {
		if err := s.consumerGroup.Consume(ctx, s.topics, &s.groupHandler); err != nil {
			s.errors <- err
			return
		}
	}
}

func (s *kafkaSubscriber) Error() <-chan error {
	return s.errors
}

type groupHandler struct {
	reciever chan msg
}

func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		u := newUnion()
		err := u.decode(msg.Value)

		// if message is not a union, ignore it and mark it as consumed
		if err != nil {
			sess.MarkMessage(msg, "")
			return err
		}

		// if message is a union retrieve the messages from it
		// and send them to the reciever channel

		// TODO: for guarante of exactly delivery once
		// we need to save messages in a persistent storage before marking them as consumed
		// and after proccessing messages delete therm from the persistent storage
		for _, ms := range u.Messages {
			for _, m := range ms {
				h.reciever <- m
			}
		}

		// after all union messages are sent to the reciever channel
		// mark the message as consumed
		sess.MarkMessage(msg, "")

	}
	return nil
}
