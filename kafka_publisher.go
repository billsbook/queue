package queue

import (
	"github.com/Shopify/sarama"
)

type kafkaPublisher struct {
	P sarama.SyncProducer
}

// NewKafkaPublisher creates a new kafka publisher
func NewKafkaPublisher(brokers []string) (Publisher, error) {
	p, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return nil, err
	}
	return &kafkaPublisher{P: p}, nil
}

// Note: all messages are sent to the same topic
// so all messages Target method must return the same value
func (p *kafkaPublisher) Publish(msgs ...msg) error {
	u := newUnion()

	t0 := msgs[0].target()
	for _, m := range msgs {
		if m.target() != t0 || m.target() == "" {
			return errTargetMismatch
		}
		u.Messages[m.msgType()] = append(u.Messages[m.msgType()], m)
	}

	pm := &sarama.ProducerMessage{
		Topic: t0,
		Value: u,
	}
	_, _, err := p.P.SendMessage(pm)

	return err
}
