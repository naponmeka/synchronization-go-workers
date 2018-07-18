package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/fatih/color"
	amqputil "github.com/naponmeka/synchronization_go_workers/amqputil"
	"github.com/streadway/amqp"
)

// CancelJob is ...
type CancelJob struct {
	Command      string `json:"command"`
	ConsumerName string `json:"consumer_name,omitempty"`
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func randPrinter() (c *color.Color) {
	num := rand.Intn(5)
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

func dowork(j amqp.Delivery, stopChan chan bool, sem chan bool) {
	p := randPrinter()
	counter := 0
dowork:
	for {
		select {
		case _, more := <-stopChan:
			if !more {
				break dowork
			}
		default:
			if counter < 100 {
				counter++
				time.Sleep(1 * time.Second)
				p.Println(string(j.Body))
			}
		}
	}
	j.Ack(false)
	<-sem
}

func main() {
	fmt.Println("Worker v2 started.")
	rabbitmqURI := "amqp://root:root@localhost:5672/"
	conn, ch, _, jobs := amqputil.CreateConsumerConnection(rabbitmqURI, "v2_jobs", 1)
	defer func() {
		conn.Close()
		ch.Close()
	}()

	cancelConn, cancelCh, cancelQueue, cancelJobs := amqputil.CreateSubscriberConnection(rabbitmqURI, "fanout_cancel")
	defer func() {
		cancelCh.Close()
		cancelConn.Close()
	}()

	cancelBroadcasterConn, cancelBroadcasterCh := amqputil.CreateFanOutPublisherConnection(rabbitmqURI, "fanout_cancel")
	defer func() {
		cancelBroadcasterCh.Close()
		cancelBroadcasterConn.Close()
	}()

	forever := make(chan bool)
	stopChan := make(chan bool)
	sem := make(chan bool, 1)
	currentCommand := ""
	go func() {
		for {
			select {
			case j := <-jobs:
				currentCommand = string(j.Body)
				cancelJob := CancelJob{Command: string(j.Body), ConsumerName: cancelQueue.Name}
				cancelBytes, _ := json.Marshal(cancelJob)
				cancelBroadcasterCh.Publish(
					"fanout_cancel", // exchange
					"",              // routing key
					false,           // mandatory
					false,           // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        cancelBytes,
					})
				sem <- true
				go dowork(j, stopChan, sem)
			case cj := <-cancelJobs:
				cancelJob := CancelJob{}
				json.Unmarshal(cj.Body, &cancelJob)
				if cancelJob.ConsumerName != cancelQueue.Name && cancelJob.Command == currentCommand {
					// Going to Cancelled routine
					// (New job running on diff machine)
					fmt.Println("Stop current job, same job with new state running in different machine")
					time.Sleep(3 * time.Second)
					close(stopChan)
					stopChan = make(chan bool)
				}
			}
		}
	}()
	<-forever
}
