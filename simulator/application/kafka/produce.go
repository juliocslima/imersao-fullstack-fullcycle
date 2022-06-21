package kafka

import (
	"encoding/json"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	route2 "github.com/juliocslima/imersao-fullstack-fullcycle-simulator/application/route"
	"github.com/juliocslima/imersao-fullstack-fullcycle-simulator/infra/kafka"
	"log"
	"os"
	"time"
)

// {"clientId": "1", "routeId": "1"}
func Produce(message *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := route2.NewRoute()
	json.Unmarshal(message.Value, &route)
	route.LoadPositions()
	positions, err := route.ExportJsonPositions()

	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}
}
