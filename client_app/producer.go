package main

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ProduceMessages(
	ctx context.Context,
	config *kafka.ConfigMap,
	topic string,
	messages <-chan UserAction,
	serializer *Serializer[UserAction]) error {

	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Producer %s: Ошибка при создании продьюсера: %v\n", topic, err)
	}
	defer p.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-messages:
			bytes := serializer.Serialize(message)

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(message.UserId),
				Value: bytes,
			}, nil)

			if err != nil {
				log.Printf("Producer %s: Ошибка при отправке сообщения: %v\n", topic, err)
			}
		}
	}
}
