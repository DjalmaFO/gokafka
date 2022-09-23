package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	if err := Publish("mensagem", "teste", producer, nil, deliveryChan); err != nil {
		log.Printf("Falha ao publicar a mensagem: %s", err.Error())
	}

	evento := <-deliveryChan

	msg := evento.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		log.Println(msg.TopicPartition.Error.Error())
		fmt.Println("Erro ao enviar a mensagem")
	} else {
		fmt.Println("Mensagem enviada: ", msg.TopicPartition.String())
	}

	// Tempo de espera pós publicação
	// Para evitar a saida do programa sem que antes a mensagem seja publicada
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
