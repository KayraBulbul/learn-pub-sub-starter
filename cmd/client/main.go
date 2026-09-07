package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/KayraBulbul/learn-pub-sub-starter/internal/gamelogic"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/pubsub"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("error making connection")
	}
	defer connection.Close()

	fmt.Println("Peril server connected!")
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("error welcoming client")
	}

	pauseQueueName := fmt.Sprintf("pause.%s", username)
	state := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Transient, handlerPause(state))
	if err != nil {
		log.Fatal("error subscribing to pause queue")
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("error creating channel")
	}
	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, moveQueueName, "army_moves.*", pubsub.Transient, handlerMove(state, channel))
	if err != nil {
		log.Fatal("error subscribing to move queue")
	}
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, "war", "war.*", pubsub.Durable, handlerWar(state, channel))
	if err != nil {
		log.Fatal("error subscribing to war queue")
	}

inputLoop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			err = state.CommandSpawn(input)
			if err != nil {
				fmt.Println("invalid arguments")
			}
		case "move":
			move, err := state.CommandMove(input)
			if err != nil {
				fmt.Println("invalid arguments")
			} else {
				err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, moveQueueName, move)
				if err != nil {
					fmt.Println("invalid input")
				} else {
					fmt.Println("army moved!")
				}
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break inputLoop
		default:
			fmt.Println("invalid command")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down connection...")
}
