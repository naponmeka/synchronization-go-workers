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

func dowork(j amqp.Delivery, stopChan chan bool, doneChan chan string) {
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
}

func main() {
	fmt.Println("Worker v3 started.")
	rabbitmqURI := "amqp://root:root@localhost:5672/"
	conn, ch, _, jobs := amqputil.CreateConsumerConnection(rabbitmqURI, "v3_jobs", 3)
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

	runningRoutines := make(map[string]chan bool)
	doneChan := make(chan string)
	go func() {
		for {
			select {
			case cmd := <-doneChan:
				close(runningRoutines[cmd])
				delete(runningRoutines, cmd)
			case j := <-jobs:
				cmd := string(j.Body)
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
				if stopChan, exist := runningRoutines[cmd]; exist {
					fmt.Println("Stop routine (same job with new state running on same machine)", cancelJob.Command)
					close(stopChan)
					time.Sleep(3 * time.Second)
				}
				runningRoutines[cmd] = make(chan bool)
				go dowork(j, runningRoutines[cmd], doneChan)
			case cj := <-cancelJobs:
				cancelJob := CancelJob{}
				json.Unmarshal(cj.Body, &cancelJob)
				if cancelJob.ConsumerName != cancelQueue.Name {
					if routineChan, exist := runningRoutines[cancelJob.Command]; exist {
						fmt.Println("Stop routine (same job with new state running on differrent machine)", cancelJob.Command)
						close(routineChan)
						delete(runningRoutines, cancelJob.Command)
						time.Sleep(3 * time.Second)
					}
				}
			}
		}
	}()
	<-forever
}
