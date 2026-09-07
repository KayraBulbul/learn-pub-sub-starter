package main

import (
	"fmt"
	"time"

	"github.com/KayraBulbul/learn-pub-sub-starter/internal/gamelogic"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/pubsub"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(ch *amqp.Channel, gl *routing.GameLog, gs *gamelogic.GameState) error {
	key := fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.Player.Username)
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, gl)
	if err != nil {
		return err
	}

	return nil
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		winMessage := fmt.Sprintf("%s won a war against %s", winner, loser)
		drawMessage := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     winMessage,
				Username:    gs.Player.Username,
			}
			if err := publishGameLog(ch, &gl, gs); err != nil {
				return pubsub.NackRequeue
			} else {
				return pubsub.Ack
			}
		case gamelogic.WarOutcomeYouWon:
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     winMessage,
				Username:    gs.Player.Username,
			}
			if err := publishGameLog(ch, &gl, gs); err != nil {
				return pubsub.NackRequeue
			} else {
				return pubsub.Ack
			}
		case gamelogic.WarOutcomeDraw:
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     drawMessage,
				Username:    gs.Player.Username,
			}
			if err := publishGameLog(ch, &gl, gs); err != nil {
				return pubsub.NackRequeue
			} else {
				return pubsub.Ack
			}
		default:
			fmt.Println("error handling war outcome")
			return pubsub.NackDiscard
		}
	}
}
