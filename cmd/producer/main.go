package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Inicio projeto")

}

func NewKafkaProducer() *kafka.Producer {
	// Cria mapa de configuração
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "gokafka-kafka-1:9092",
	}

	// Gera um novo kafka-producer (Produtor de conteudo)
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}
