package main

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	kafka2 "github.com/juliocslima/imersao-fullstack-fullcycle-simulator/application/kafka"
	"github.com/juliocslima/imersao-fullstack-fullcycle-simulator/infra/kafka"
	"log"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error load .env file")
	}
}

func main() {
	messageChannel := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(messageChannel)
	go consumer.Consume()

	for message := range messageChannel {
		fmt.Println(string(message.Value))
		go kafka2.Produce(message)
	}
}
