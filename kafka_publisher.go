package queue

import "github.com/Shopify/sarama"

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
func (p *kafkaPublisher) Publish(msgs map[int]Msg) error {
	u := newUnion()

	t0 := msgs[0].Target()
	for i, m := range msgs {
		if m.Target() != t0 {
			return ErrTargetMismatch
		}
		u.Messages[m.MsgType()][i] = m
	}

	pm := &sarama.ProducerMessage{
		Topic: t0,
		Value: u,
	}
	_, _, err := p.P.SendMessage(pm)

	return err
}
