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

	queueName := fmt.Sprintf("pause.%s", username)
	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatal("error declare and bind")
	}

	state := gamelogic.NewGameState(username)

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
			_, err = state.CommandMove(input)
			if err != nil {
				fmt.Println("invalid arguments")
			} else {
				fmt.Println("army moved!")
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
