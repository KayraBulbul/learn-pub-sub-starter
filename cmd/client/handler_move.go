package main

import (
	"fmt"

	"github.com/KayraBulbul/learn-pub-sub-starter/internal/gamelogic"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/pubsub"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(am gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			key := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username)
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Println("error publishing move")
			return pubsub.NackRequeue
			} else {
				return pubsub.Ack
			}
		default:
			return pubsub.NackDiscard
		}
	}
}
