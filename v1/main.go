package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/fatih/color"
	amqputil "github.com/naponmeka/synchronization_go_workers/amqputil"
	"github.com/streadway/amqp"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func randPrinter() (c *color.Color) {
	num := rand.Intn(5)
	fmt.Println(num)
	if num == 1 {
		c = color.New(color.FgRed, color.Bold)
	} else if num == 2 {
		c = color.New(color.FgGreen, color.Bold)
	} else if num == 3 {
		c = color.New(color.FgYellow, color.Bold)
	} else if num == 4 {
		c = color.New(color.FgBlue, color.Bold)
	} else if num == 5 {
		c = color.New(color.FgMagenta, color.Bold)
	} else {
		c = color.New(color.FgWhite, color.Bold)
	}
	return c
}

func dowork(j amqp.Delivery) {
	p := randPrinter()
	for i := 0; i < 100; i++ {
		time.Sleep(1 * time.Second)
		p.Println(string(j.Body))
	}
	j.Ack(false)
}

func main() {
	fmt.Println("Worker v1 started.")
	rabbitmqURI := "amqp://root:root@localhost:5672/"
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
