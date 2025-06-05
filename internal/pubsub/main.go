package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DurableQueue int = iota
	TransientQueue
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange, queueName, key string,
	simpleQueueType int, // enum to represent `durable` or `transient`
) (*amqp.Channel, amqp.Queue, error) {
	durable, autoDelete, exclusive := false, false, false
	if simpleQueueType == DurableQueue {
		durable = true
	} else if simpleQueueType == TransientQueue {
		autoDelete = true
		exclusive = true
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	if err = ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return ch, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
