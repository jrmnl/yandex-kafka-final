package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ProduceMessages(
	config *kafka.ConfigMap,
	topic string,
	messages []Product,
	serializer *Serializer[Product]) error {

	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Producer %s: Ошибка при создании продьюсера: %v\n", topic, err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)
	for _, message := range messages {
		bytes := serializer.Serialize(message)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(message.ProductID),
			Value: bytes,
		}, deliveryChan)

		if err != nil {
			log.Printf("Producer %s: Ошибка при отправке сообщения: %v\n", topic, err)
			continue
		}
	}

	for range messages {
		event := <-deliveryChan
		msg := event.(*kafka.Message)

		if msg.TopicPartition.Error != nil {
			log.Printf("Producer %s: Ошибка доставки сообщения: %v\n", topic, msg.TopicPartition.Error)
			return msg.TopicPartition.Error
		}
	}

	return nil
}
