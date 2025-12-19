package main

import (
	"context"
	"embed"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-gonic/gin"
	"github.com/jrmnl/yandex-kafka-final-client-app/api"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/swaggest/swgui/v5emb"
	"golang.org/x/sync/errgroup"
)

//go:embed api/openapi.yaml
var content embed.FS

func NewGinServer(handler api.ServerInterface, port string) *http.Server {
	swagger, err := api.GetSwagger()
	if err != nil {
		log.Fatalln("Ошибка получения сваггера")
	}

	swUi := v5emb.New(
		"YandexFinal-ClientApp",
		"/openapi.yaml",
		"/api/docs/",
	)

	// This is how you set up a basic gin router
	r := gin.Default()

	apiGroup := r.Group("/api/docs/")
	{
		apiGroup.Any("/*any", gin.WrapH(swUi))
	}

	r.StaticFileFS("/openapi.yaml", "api/openapi.yaml", http.FS(content))
	r.Use(middleware.OapiRequestValidator(swagger))

	api.RegisterHandlers(r, handler)

	s := &http.Server{
		Handler: r,
		Addr:    net.JoinHostPort("0.0.0.0", port),
	}
	return s
}

func main() {
	log.Println("Запущено")
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	settings := getAppSettings()

	log.Println("Ожидаем создание топиков")
	producerCfg := getProducerConfig(settings)
	WaitTopic(ctx, producerCfg, settings.ClientActionsTopic)
	log.Println("Ожидание топиков завершено")

	log.Println("Ожидаем доступность реджистри")
	registryConfig := getRegistryConfig(settings)
	WaitRegistry(ctx, registryConfig)
	log.Println("реджистри доступно")

	g, _ := errgroup.WithContext(ctx)

	userActions := make(chan UserAction, 1)
	// запускаем продьюсер
	g.Go(func() error {
		serializer := NewSerializer[UserAction](registryConfig, settings.ClientActionsTopic)
		err := ProduceMessages(
			ctx,
			producerCfg,
			settings.ClientActionsTopic,
			userActions,
			serializer,
		)
		return err
	})

	// запускаем вебсервер
	g.Go(func() error {
		es, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{settings.ElasticsearchUrl},
		})
		if err != nil {
			log.Fatalf("эластик не получилось проинициилизировать %v\n", err)
		}

		handler := NewApiHandler(es, userActions)
		server := NewGinServer(handler, "8098")
		err = server.ListenAndServe()
		return err
	})

	err := g.Wait()
	log.Println(err)
	log.Println("Завершено")
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
		"sasl.username":            settings.Username,
		"sasl.password":            settings.Password,
	}
}

func getRegistryConfig(settings AppSettings) *schemaregistry.Config {
	return schemaregistry.NewConfig(settings.SchemaRegistryUrl)
}

type AppSettings struct {
	BootstrapServers   string
	SchemaRegistryUrl  string
	ElasticsearchUrl   string
	SslCaLocation      string
	ClientActionsTopic string
	Username           string
	Password           string
}

func getAppSettings() AppSettings {
	return AppSettings{
		BootstrapServers:   getEnv("BOOTSTRAP_SERVERS", "kafka-1:19093,kafka-2:19093,kafka-3:19093"),
		SchemaRegistryUrl:  getEnv("REGISTRY", "http://schema-registry:8080/"),
		ElasticsearchUrl:   getEnv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
		SslCaLocation:      getEnv("SSL_CA_LOCATION", "./ca.crt"),
		ClientActionsTopic: getEnv("TOPIC", "client-actions"),
		Username:           getEnv("USERNAME", "client_app"),
		Password:           getEnv("PASSWORD", "client_app-secret"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
