package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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
	if err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		unMarshallJSON,
	); err != nil {
		return err
	}

	return nil
}

func unMarshallJSON[T any](b []byte) (T, error) {
	var body T
	buffer := bytes.NewBuffer(b)
	enc := json.NewDecoder(buffer)
	if err := enc.Decode(&body); err != nil {
		return body, err
	}

	return body, nil
}

func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	if err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
		handler,
		unMarshallGob,
	); err != nil {
		return err
	}

	return nil
}

func unMarshallGob[T any](b []byte) (T, error) {
	var body T

	buffer := bytes.NewBuffer(b)
	enc := gob.NewDecoder(buffer)
	if err := enc.Decode(&body); err != nil {
		return body, err
	}

	return body, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unMarshaller func([]byte) (T, error),
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

	if err = ch.Qos(10, 0, false); err != nil {
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
			body, err := unMarshaller(delivery.Body)
			if err != nil {
				log.Println(err)
				continue
			}

			switch handler(body) {
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(val); err != nil {
		return err
	}

	if err := ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        b.Bytes(),
		},
	); err != nil {
		return err
	}

	return nil
}
