package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType uint

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue         = "nackRequeue"
	NackDiscard         = "nackDiscard"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange, queueName, key string,
	simpleQueueType SimpleQueueType, // enum to represent `durable` or `transient`
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
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"},
	)
	if err != nil {
		return ch, amqp.Queue{}, err
	}

	if err = ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return ch, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
	)
	if err != nil {
		return err
	}

	deliveryCh, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryCh {
			var body T
			if err = json.Unmarshal(delivery.Body, &body); err != nil {
				log.Println(err)
				continue
			}

			acktype := handler(body)
			log.Println(acktype)

			switch acktype {
			case Ack:
				if err = delivery.Ack(false); err != nil {
					log.Println(err)
					continue
				}
			case NackRequeue:
				if err = delivery.Nack(false, true); err != nil {
					log.Println(err)
					continue
				}
			case NackDiscard:
				if err = delivery.Nack(false, false); err != nil {
					log.Println(err)
					continue
				}
			}
		}
	}()

	return nil
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
