package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/billsbook/queue/types"
	"github.com/matryer/try"
)

type kafkaPublisher struct {
	producer sarama.SyncProducer
	brokers  []string
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
		brokers:  brokers,
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
		fmt.Println("error publishing message: ", err)
		p.store.saveMsg(msg)
	}
	return err
}

// RetryEngine starts a retry engine that will retry publishing failed messages
// must be runed in a separate goroutine
func (p *kafkaPublisher) RetryEngine(wg *sync.WaitGroup) {
	defer wg.Done()

start:
	for {
		select {

		case <-p.store.engine.activate:

			if atomic.LoadInt64(&p.store.engine.isActive) == 0 {
				atomic.StoreInt64(&p.store.engine.isActive, 1)
				try.MaxRetries = p.store.engine.maxRetries

				err := try.Do(func(attempt int) (bool, error) {
					fmt.Println("connecting to kafka...")
					// ping kafka
					pr, err := sarama.NewSyncProducer(p.brokers, nil)

					if err == nil {
						p.producer = pr
						fmt.Println("connected to kafka")
						return false, nil
					}
					time.Sleep(time.Duration(attempt) * time.Second)

					p.store.engine.attempts = attempt
					return attempt < p.store.engine.maxRetries, err
				})

				if err != nil {
					atomic.StoreInt64(&p.store.engine.isActive, 0)
					// TODO: implement a mechanism for this situation
					panic(err)
				}

				// retry publishing the failed message
				for {
					msg, err := p.store.retrieveMsg()
					if err != nil {
						break
					}
					fmt.Printf("retrying publishing message: %+v\n", msg.ID())

					p.Publish(msg)
					p.store.remove(msg)

				}
				atomic.StoreInt64(&p.store.engine.isActive, 0)
				continue start

			}
		}

	}
}
