package main

import (
	"fmt"
	"time"

	amqputil "github.com/naponmeka/synchronization_go_workers/amqputil"
	"github.com/streadway/amqp"
)

func dowork(j amqp.Delivery) {
	for i := 0; i < 100; i++ {
		time.Sleep(1 * time.Second)
		fmt.Println(string(j.Body))
	}
	j.Ack(false)
}

func main() {
	fmt.Println("Worker v1 started.")
	rabbitmqURI := "amqp://root:pass@localhost:5672/"
	conn, ch, _, jobs := amqputil.CreateConsumerConnection(rabbitmqURI, "v1_jobs", 1)
	defer func() {
		conn.Close()
		ch.Close()
	}()

	forever := make(chan bool)
	go func() {
		for j := range jobs {
			dowork(j)
		}
	}()
	<-forever
}
