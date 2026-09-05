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
	str, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("error welcoming client")
	}

	queueName := fmt.Sprintf("pause.%s", str)
	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatal("error declare and bind")
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down connection...")
}
