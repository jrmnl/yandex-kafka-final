package main

import (
	"context"
	"log"
	"maps"
	"slices"
	"time"

	"github.com/IBM/sarama"
)

func WaitTopics(ctx context.Context, brokers []string, config *sarama.Config, expectedTopics []string) {
	for {
		time.Sleep(2 * time.Second)
		select {
		case <-ctx.Done():
			return
		default:
			admin, err := sarama.NewClusterAdmin(brokers, config)
			if err != nil {
				log.Printf("ошибка создания админ клиента: %v", err)
				continue
			}
			defer admin.Close()

			topics, err := admin.ListTopics()
			if err != nil {
				log.Printf("ошибка при получении топиков: %v", err)
			} else {
				log.Printf("получены топики: %v", topics)
				keys := slices.Collect(maps.Keys(topics))

				allExists := true
				for _, t := range expectedTopics {
					if !slices.Contains(keys, t) {
						log.Printf("топик %s еще не создан", t)
						allExists = false
						break
					}
				}

				if allExists {
					return
				}
			}
		}
	}
}
