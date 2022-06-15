package main

import (
	"fmt"
	"sync"

	"github.com/billsbook/queue"
)

func main() {

	// publisher, err := queue.NewKafkaPublisher([]string{"localhost:9092"})
	// if err != nil {
	// 	panic(err)
	// }

	// err = publisher.Publish(queue.CreateConsumerMsg("ali", "alikarimi@gmail.com"))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Published")

	subscriber, err := queue.NewKafkaSubscriber(queue.CostumerGroup, []string{"localhost:9092"}, []string{"costumer"})
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
		case err = <-subscriber.Error():
			fmt.Println(err)
			wg.Wait()

		}
	}

}
