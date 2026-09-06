package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](conn *amqp.Connection,
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
		var data T
		for message := range deliveries {
			err = json.Unmarshal(message.Body, &data)
			if err != nil {
				fmt.Println("error unmarshaling")
				continue
			}

			acknowledgeType := handler(data)
			switch acknowledgeType {
			case Ack:
				err = message.Ack(false)
				if err != nil {
					fmt.Println("error acknowledging")
					return
				}
				fmt.Println("message acknowledged")
			case NackRequeue:
				err = message.Nack(false, true)
				if err != nil {
					fmt.Println("error negative acknowledging and requeuing")
					return
				}
				fmt.Println("message negative acknowledged and requeued")
			case NackDiscard:
				err = message.Nack(false, false)
				if err != nil {
					fmt.Println("error negative acknowledging and discarding")
					return
				}
				fmt.Println("message negative acknowledged and discarded")
			}
		}
	}()

	return nil
}
