package queue

import (
	"github.com/Shopify/sarama"
	"github.com/billsbook/queue/types"
)

type kafkaPublisher struct {
	producer sarama.SyncProducer
	store    *pubStore
}

// NewKafkaPublisher creates a new kafka publisher
func NewKafkaPublisher(brokers []string, service Service) (Publisher, error) {
	p, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return nil, err
	}
	return &kafkaPublisher{
		producer: p,
		store:    newPubStore(service),
	}, nil
}

// Note: all messages are sent to the same topic
// so all messages Target method must return the same value
func (p *kafkaPublisher) Publish(msg types.Msg) error {

	pm := &sarama.ProducerMessage{
		Topic: msg.Target(),
		Value: msg,
	}
	_, _, err := p.producer.SendMessage(pm)
	if err != nil {
		return p.store.SaveMsg(msg)
	}
	return nil
}
