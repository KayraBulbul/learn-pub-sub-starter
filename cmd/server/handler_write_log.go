package main

import (
	"fmt"

	"github.com/KayraBulbul/learn-pub-sub-starter/internal/gamelogic"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/pubsub"
	"github.com/KayraBulbul/learn-pub-sub-starter/internal/routing"
)

func handlerWriteLog(gl routing.GameLog) pubsub.Acktype {
	defer fmt.Print("> ")
	err := gamelogic.WriteLog(gl)
	if err != nil {
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
