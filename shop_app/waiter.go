package main

import (
	"context"
	"log"
	"slices"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// Ждет создание топика в кафке
func WaitTopic(ctx context.Context, config *kafka.ConfigMap, topic string) {
	admin, err := kafka.NewAdminClient(config)
	if err != nil {
		log.Fatalf("Не создать AdminClient: %v\n", err)
	}
	defer admin.Close()

	for {
		select {
		case <-ctx.Done():
			log.Fatalln("Отмена приложения")
		default:
			if checkTopicExists(admin, topic) {
				if checkTopicWrite(ctx, admin, topic) {
					return
				}
			}
			log.Printf("Ожидаем создание топика %s\n", topic)
			time.Sleep(2 * time.Second)
		}
	}
}
func checkTopicExists(admin *kafka.AdminClient, topic string) bool {
	metadata, err := admin.GetMetadata(&topic, false, 200)
	if err != nil {
		log.Printf("Waiter: не получилось получить метаданные %v\n", err)
		return false
	}

	_, exists := metadata.Topics[topic]
	return exists
}

func checkTopicWrite(ctx context.Context, admin *kafka.AdminClient, topic string) bool {
	topics := kafka.NewTopicCollectionOfTopicNames([]string{topic})
	results, err := admin.DescribeTopics(ctx, topics, kafka.SetAdminOptionIncludeAuthorizedOperations(true))

	if err != nil {
		log.Printf("Waiter: не получилось получить информацию по ACL %v\n", err)
		return false
	}

	info := results.TopicDescriptions[0]
	return slices.Contains(info.AuthorizedOperations, kafka.ACLOperationWrite)
}

func WaitRegistry(ctx context.Context, cfg *schemaregistry.Config) {
	client, err := schemaregistry.NewClient(cfg)
	if err != nil {
		log.Fatalf("Ошибка при подключении к Schema Registry: %v\n", err)
	}
	defer client.Close()

	for {
		select {
		case <-ctx.Done():
			log.Fatalln("Отмена приложения")
		default:
			_, err := client.GetAllSubjects()
			if err != nil {
				log.Printf("Waiter: не удалось получить данные из Schema Registry: %v\n", err)
				log.Println("Ожидаем доступа к Registry")
				time.Sleep(2 * time.Second)
			} else {
				return
			}
		}
	}
}
