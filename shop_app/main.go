package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type AppSettings struct {
	BootstrapServers  string
	SchemaRegistryUrl string
	SslCaLocation     string
	ProductsTopic     string
	ProducerUsername  string
	ProducerPassword  string
	FilePath          string
}

func getAppSettings() AppSettings {
	return AppSettings{
		BootstrapServers:  getEnv("BOOTSTRAP_SERVERS", "kafka-1:19093,kafka-2:19093,kafka-3:19093"),
		SchemaRegistryUrl: getEnv("REGISTRY", "http://schema-registry:8080/"),
		SslCaLocation:     getEnv("SSL_CA_LOCATION", "./ca.crt"),
		ProductsTopic:     getEnv("TOPIC", "products-raw"),
		ProducerUsername:  getEnv("PRODUCER_USERNAME", "shop_app"),
		ProducerPassword:  getEnv("PRODUCER_PASSWORD", "shop_app-secret"),
		FilePath:          getEnv("FILE_PATH", "data.json"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	settings := getAppSettings()

	products := getProducts(settings.FilePath)
	producerConfig := getProducerConfig(settings)
	WaitTopic(ctx, producerConfig, settings.ProductsTopic)
	registryConfig := getRegistryConfig(settings)
	WaitRegistry(ctx, registryConfig)

	serializer := NewSerializer[Product](registryConfig, settings.ProductsTopic)

	ProduceMessages(producerConfig, settings.ProductsTopic, products, serializer)
	log.Println("Все сообщения из файла отправлены")
}

func getProducerConfig(settings AppSettings) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":        settings.BootstrapServers,
		"ssl.ca.location":          settings.SslCaLocation,
		"message.timeout.ms":       10000,
		"message.send.max.retries": 3,
		"acks":                     "all",
		"security.protocol":        "SASL_SSL",
		"sasl.mechanism":           "PLAIN",
		"sasl.username":            settings.ProducerUsername,
		"sasl.password":            settings.ProducerPassword,
	}
}

func getRegistryConfig(settings AppSettings) *schemaregistry.Config {
	return schemaregistry.NewConfig(settings.SchemaRegistryUrl)
}

func getProducts(path string) []Product {
	bytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	var products []Product
	err = json.Unmarshal(bytes, &products)
	if err != nil {
		log.Fatal(err)
	}
	return products
}
