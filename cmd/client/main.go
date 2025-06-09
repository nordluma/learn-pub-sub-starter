package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	log.Println("Connected to RabbitMQ")

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, name),
		routing.PauseKey,
		pubsub.TransientQueue,
	)
	if err != nil {
		log.Fatal(err)
	}

	state := gamelogic.NewGameState(name)

	quit := false
	for {
		cmds := gamelogic.GetInput()
		if len(cmds) == 0 {
			continue
		}

		switch cmds[0] {
		case "spawn":
			if err = state.CommandSpawn(cmds); err != nil {
				log.Println(err)
			}
			break
		case "move":
			_, err := state.CommandMove(cmds)
			if err != nil {
				log.Println(err)
			}
			break
		case "status":
			state.CommandStatus()
			break
		case "help":
			gamelogic.PrintClientHelp()
			break
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			quit = true
			break
		default:
			fmt.Printf("unknown command '%s'\n", cmds[0])
			continue
		}

		if quit {
			break
		}
	}

	log.Println("Shutting down Peril client")
}
