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
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()
	connectionString := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("error making connection")
	}
	defer connection.Close()

	fmt.Println("Peril server connected!")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("error making new channel")
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		if input[0] == "pause" {
			fmt.Println("sending pause message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatal("error publishing JSON")
			}
		} else if input[0] == "resume" {
			fmt.Println("sending resume message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatal("error publishing JSON")
			}
		} else if input[0] == "quit" {
			break
		} else {
			fmt.Println("unknown command")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down connection...")
}
