package amqputil

import (
	"github.com/streadway/amqp"
)

func CreatePublisherConnection(uri, queueName string) (*amqp.Connection, *amqp.Channel, amqp.Queue) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return conn, ch, q
}

func CreateFanOutPublisherConnection(uri, exchangeName string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	return conn, ch
}
