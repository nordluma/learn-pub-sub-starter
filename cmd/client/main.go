package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			log.Printf("unknown move '%v'", moveOutcome)
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(
	gs *gamelogic.GameState, ch *amqp.Channel,
) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		var ackType pubsub.AckType
		var message string
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			ackType = pubsub.Ack
			message = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeYouWon:
			ackType = pubsub.Ack
			message = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeDraw:
			ackType = pubsub.Ack
			message = fmt.Sprintf(
				"A war between %s and %s resulted in a draw",
				winner,
				loser,
			)
		default:
			log.Printf("unknown war outcome '%v'", outcome)
			return pubsub.NackDiscard
		}

		if err := pubsub.PublishGob(
			ch,
			routing.ExchangePerilTopic,
			fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.GetUsername()),
			routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    gs.GetUsername(),
			},
		); err != nil {
			return pubsub.NackRequeue
		}

		return ackType
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
		handlerWar(gameState, ch),
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
			if len(cmds) < 2 {
				log.Println("an integer has to be specified")
				continue
			}
			num, err := strconv.Atoi(cmds[1])
			if err != nil {
				log.Println(err)
				continue
			}

			for range num {
				if err = pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     gamelogic.GetMaliciousLog(),
						Username:    username,
					}); err != nil {
					log.Println(err)
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("unknown command '%s'\n", cmds[0])
		}
	}
}
