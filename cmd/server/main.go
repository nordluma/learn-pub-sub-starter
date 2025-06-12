package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerLog(gl routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	if err := gamelogic.WriteLog(gl); err != nil {
		return pubsub.NackDiscard
	}

	return pubsub.Ack
}

func main() {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting Peril server...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	log.Println("Connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.DurableQueue,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err = pubsub.SubscribeGOB(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.DurableQueue,
		handlerLog,
	); err != nil {
		log.Fatal(err)
	}

	gamelogic.PrintServerHelp()

	for {
		cmds := gamelogic.GetInput()
		if len(cmds) == 0 {
			continue
		}

		switch cmds[0] {
		case "pause":
			fmt.Println("sending pause message")
			if err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			); err != nil {
				log.Fatal(err)
				continue
			}
		case "resume":
			fmt.Println("sending resume message")
			if err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			); err != nil {
				log.Fatal(err)
				continue
			}
		case "quit":
			fmt.Println("exiting game")
			return
		default:
			fmt.Printf("unknown command '%s'\n", cmds[0])
		}

	}
}
