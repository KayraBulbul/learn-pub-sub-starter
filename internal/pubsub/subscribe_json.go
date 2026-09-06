package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
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

			handler(data)
			err = message.Ack(false)
			if err != nil {
				fmt.Println("error acknowledging")
				return
			}
		}
	}()

	return nil
}
