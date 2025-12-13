package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type AppSettings struct {
	BootstrapServers       string
	SchemaRegistryUrl      string
	SslCaLocation          string
	SslCertificateLocation string
	SslKeyLocation         string
	ProductsTopic          string
	ProducerUsername       string
	ProducerPassword       string
	FilePath               string
}

func getAppSettings() AppSettings {
	return AppSettings{
		BootstrapServers:       getEnv("BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
		SchemaRegistryUrl:      getEnv("REGISTRY", "http://schema-registry:8080/"),
		SslCaLocation:          getEnv("SSL_CA_LOCATION", "./ca.crt"),
		SslCertificateLocation: getEnv("SSL_CERTIFICATE_LOCATION", "./kafka-1-creds/kafka-1.crt"),
		SslKeyLocation:         getEnv("SSL_KEY_LOCATION", "./kafka-1-creds/kafka-1.key"),
		ProductsTopic:          getEnv("TOPIC_1", "products-raw"),
		ProducerUsername:       getEnv("PRODUCER_USERNAME", "producer"),
		ProducerPassword:       getEnv("PRODUCER_PASSWORD", "producer-secret"),
		FilePath:               getEnv("FILE_PATH", "data.json"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	settings := getAppSettings()

	products := getProducts(settings.FilePath)
	producerConfig := getProducerConfig(settings)

	serializer := NewSerializer[Product](getRegistryConfig(settings), settings.ProductsTopic)

	ProduceMessages(producerConfig, settings.ProductsTopic, products, serializer)
}

func getProducerConfig(settings AppSettings) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": settings.BootstrapServers,
		//"ssl.ca.location":          settings.SslCaLocation,
		// "ssl.certificate.location": settings.SslCertificateLocation,
		// "ssl.key.location":         settings.SslKeyLocation,
		"message.timeout.ms":       10000,
		"message.send.max.retries": 3,
		"acks":                     "all",
		//"security.protocol":        "SASL_SSL",
		//"sasl.mechanism":           "PLAIN",
		//"sasl.username":            settings.ProducerUsername,
		//"sasl.password":            settings.ProducerPassword,
	}
}

func getRegistryConfig(settings AppSettings) *schemaregistry.Config {
	/*if settings.UseSasl {
		serConfig := schemaregistry.NewConfigWithBasicAuthentication(
			settings.SchemaRegistryUrl,
			settings.ProducerUser,
			settings.ProducerPassword)
		serConfig.SslCaLocation = settings.CertificatePath
		return serConfig
	}*/

	return schemaregistry.NewConfig(settings.SchemaRegistryUrl)
}

func getProducts(path string) []Product {
	bytes, err := os.ReadFile("data.json")
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
