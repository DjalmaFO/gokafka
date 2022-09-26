package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	if err := Publish("mensagem-teste", "teste", producer, nil, deliveryChan); err != nil {
		log.Printf("Falha ao publicar a mensagem: %s", err.Error())
	}

	go DeliveryReport(deliveryChan)
	producer.Flush(1000)
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

func Publish(msg, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	m := kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	return producer.Produce(&m, deliveryChan)
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch event := e.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				log.Println(event.TopicPartition.Error.Error())
				fmt.Println("Erro ao enviar a mensagem")
			} else {
				fmt.Println("Mensagem enviada: ", event.TopicPartition.String())
			}
		}
	}
}
