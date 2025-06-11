package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(
	gs *gamelogic.GameState,
) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)

		return pubsub.Ack
	}
}

func handlerMove(
	gs *gamelogic.GameState, ch *amqp.Channel,
) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		moveOutcome := gs.HandleMove(mv)
		switch moveOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf(
					"%s.%s",
					routing.WarRecognitionsPrefix,
					gs.GetUsername(),
				),
				gamelogic.RecognitionOfWar{
					Attacker: mv.Player,
					Defender: gs.GetPlayerSnap(),
				},
			); err != nil {
				log.Printf("error: %s", err)
				return pubsub.NackRequeue
			}

			return pubsub.NackRequeue
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			log.Printf("unknown move '%v'", moveOutcome)
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(
	gs *gamelogic.GameState,
) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			log.Printf("unknown war outcome '%v'", outcome)
			return pubsub.NackDiscard
		}
	}
}

func main() {
	fmt.Println("Starting Peril client...")
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(username)

	if err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gameState),
	); err != nil {
		log.Fatal(err)
	}

	if err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.TransientQueue,
		handlerMove(gameState, ch),
	); err != nil {
		log.Fatal(err)
	}

	if err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.DurableQueue,
		handlerWar(gameState),
	); err != nil {
		log.Fatal(err)
	}

	for {
		cmds := gamelogic.GetInput()
		if len(cmds) == 0 {
			continue
		}

		switch cmds[0] {
		case "spawn":
			if err = gameState.CommandSpawn(cmds); err != nil {
				log.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(cmds)
			if err != nil {
				log.Println(err)
				continue
			}

			if err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				move,
			); err != nil {
				log.Println(err)
				continue
			}

			log.Println("move published successfully")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("unknown command '%s'\n", cmds[0])
		}
	}
}
