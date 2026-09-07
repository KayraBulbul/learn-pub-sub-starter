package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for message := range deliveries {
			buf := bytes.NewBuffer(message.Body)
			decoder := gob.NewDecoder(buf)

			var log T
			if err := decoder.Decode(&log); err != nil {
				fmt.Println("error decoding")
				continue
			}

			acknowledgeType := handler(log)
			switch acknowledgeType {
			case Ack:
				err = message.Ack(false)
				if err != nil {
					fmt.Println("error acknowledging")
				}
			case NackRequeue:
				err = message.Nack(false, true)
				if err != nil {
					fmt.Println("error negative acknowledging and requeuing")
				}
			case NackDiscard:
				err = message.Nack(false, false)
				if err != nil {
					fmt.Println("error negative acknowledging and discarding")
				}
			}
		}
	}()

	return nil
}
