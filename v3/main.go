package main

import (
	"encoding/json"
	"fmt"
	"time"

	amqputil "github.com/naponmeka/synchronization_go_workers/amqputil"
	"github.com/streadway/amqp"
)

// CancelJob is ...
type CancelJob struct {
	Command      string `json:"command"`
	ConsumerName string `json:"consumer_name,omitempty"`
}

func dowork(j amqp.Delivery, stopChan chan bool, doneChan chan string) {
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
				fmt.Println(string(j.Body))
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
					fmt.Println("Stop routine with same command(same machine)", cancelJob.Command)
					close(stopChan)
				}
				runningRoutines[cmd] = make(chan bool)
				go dowork(j, runningRoutines[cmd], doneChan)
			case cj := <-cancelJobs:
				cancelJob := CancelJob{}
				json.Unmarshal(cj.Body, &cancelJob)
				if cancelJob.ConsumerName != cancelQueue.Name {
					if routineChan, exist := runningRoutines[cancelJob.Command]; exist {
						fmt.Println("Stop routine with same command(diff machine)", cancelJob.Command)
						close(routineChan)
						delete(runningRoutines, cancelJob.Command)
					}
				}
			}
		}
	}()
	<-forever
}
