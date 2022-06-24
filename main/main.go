package main

import (
	"fmt"
	"sync"

	"github.com/billsbook/queue"
	"github.com/billsbook/queue/types"
)

func main() {

	// publisher, err := queue.NewKafkaPublisher([]string{"localhost:9092"}, queue.CustomerService)
	// if err != nil {
	// 	panic(err)
	// }

	// for i := 0; i < 10; i++ {
	// 	err = publisher.Publish(models.CreateCustomerMsg(fmt.Sprintf("%d", i), "alikarimi", "alikarimi@gmail.com"))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Println("Published")
	// }

	subscriber, err := queue.NewKafkaSubscriber(queue.CostumerGroup, queue.CustomerService, []string{"localhost:9092"}, []string{string(types.CustomerTopic)})
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go subscriber.Subscribe(wg)

	for {
		select {
		case msg := <-subscriber.Recieve():
			fmt.Println(msg.Value())
			err = subscriber.Processed(msg)
			if err != nil {
				panic(err)
			}

		case err = <-subscriber.Error():
			fmt.Println(err)
			wg.Wait()

		}
	}

}
