package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/billsbook/queue/models"
	"github.com/billsbook/queue/types"
)

type processedMsg struct {
	id  string
	msg *sarama.ConsumerMessage
}

type KafkaConsumerGroup string

const (
	CostumerGroup KafkaConsumerGroup = "customer_service"
)

// kafkaSubscriber is respoinsible for subscribing to multiple topics
//  in a cluster of consumer groups
// also implements the sarama.ConsumerGroupHandler interface
type kafkaSubscriber struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
	groupHandler  groupHandler

	store types.SubscriberStore

	errors chan error
}

// NewKafkaSubscriber creates a new kafka subscriber
func NewKafkaSubscriber(group KafkaConsumerGroup, service Service, brokers, topics []string) (Subscriber, error) {

	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		return nil, err
	}

	g, err := sarama.NewConsumerGroupFromClient(string(group), client)
	if err != nil {
		return nil, err
	}

	s := &kafkaSubscriber{
		consumerGroup: g,
		topics:        topics,
		store:         newSubStore(service),
		groupHandler: groupHandler{
			reciever:   make(chan types.Msg),
			processed:  make(chan *processedMsg),
			pendingMsg: []*sarama.ConsumerMessage{},
		}}
	s.groupHandler.subStore = s.store
	return s, nil
}

func (s *kafkaSubscriber) Recieve() <-chan types.Msg {
	return s.groupHandler.reciever
}

func (h *kafkaSubscriber) Processed(msg types.Msg) error {

	b, err := msg.Encode()
	if err != nil {
		return err
	}

	// save msg as processed
	h.store.SaveMsgID(msg)

	// remove msg from pendingMsg
	for i, m := range h.groupHandler.pendingMsg {
		if bytes.Equal(m.Value, b) {
			h.groupHandler.pendingMsg = append(h.groupHandler.pendingMsg[:i], h.groupHandler.pendingMsg[i+1:]...)
			pm := &processedMsg{
				id:  msg.ID(),
				msg: m,
			}
			h.groupHandler.processed <- pm
			return nil
		}
	}
	return errors.New("msg not found")

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
	reciever   chan types.Msg
	subStore   types.SubscriberStore
	pendingMsg []*sarama.ConsumerMessage
	processed  chan *processedMsg
}

func (h *groupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	fmt.Printf("start new session on topics: %v as member: %s\n", sess.Claims(), sess.MemberID())
	return nil
}

func (h *groupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {

	fmt.Printf("cleanup session on topics: %v as member: %s\n", sess.Claims(), sess.MemberID())
	return nil
}

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	go func(sess sarama.ConsumerGroupSession) {
		for pm := range h.processed {
			sess.MarkMessage(pm.msg, "")
			fmt.Printf("message `%s` marked as processed\n", pm.id)
		}
	}(sess)

	for msg := range claim.Messages() {
		m, err := models.DecodeMsg(msg.Value)
		// if message is not a valid, ignore it and mark it as consumed
		if err != nil || h.subStore.IsProcessed(m) {
			sess.MarkMessage(msg, "")
			continue
		}

		// add msg to pendingMsg
		h.pendingMsg = append(h.pendingMsg, msg)

		// if message is valid send it to the reciever channel
		h.reciever <- m

	}
	return nil
}
