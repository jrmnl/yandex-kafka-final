package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/gin-gonic/gin"
	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/api"
	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/codecs"
	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/events"
	"github.com/lovoo/goka"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/swaggest/swgui/v5emb"
	"golang.org/x/sync/errgroup"
)

func NewGinServer(handler api.ServerInterface, port string) *http.Server {
	swagger, err := api.GetSwagger()
	if err != nil {
		log.Fatalln("Ошибка получения сваггера")
	}

	swUi := v5emb.New(
		"YandexFinal-FilteringApp",
		"/openapi.yaml",
		"/api/docs/",
	)

	// This is how you set up a basic gin router
	r := gin.Default()

	apiGroup := r.Group("/api/docs/")
	{
		apiGroup.Any("/*any", gin.WrapH(swUi))
	}
	r.StaticFile("/openapi.yaml", "api/openapi.yaml")
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
	settings := getSettings()
	saramaCfg := getSaramaConfig(settings)
	goka.ReplaceGlobalConfig(saramaCfg)
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	log.Println("Ожидаем создание топиков")
	WaitTopics(ctx, settings.Brokers, saramaCfg, []string{
		settings.BlockedProductsGroupTableTopic,
		settings.BlockedProductsTopic,
		settings.InputProductsTopic,
		settings.OutputProductsTopic,
	})
	log.Println("Ожидание топиков завершено")

	g, ctx := errgroup.WithContext(ctx)

	registryConfig := getSchemaRegistryConfig(settings.SchemaRegistryUrl)
	blockedProductCodec := codecs.NewAvroSerializer[events.BlockedProduct](
		registryConfig,
		settings.BlockedProductsTopic)
	intSliceCodec := codecs.NewAvroSerializer[[]int](
		registryConfig,
		settings.BlockedProductsGroupTableTopic)
	inputProductCodec := codecs.NewAvroSerializer[Product](
		registryConfig,
		settings.InputProductsTopic)

	emitter, err := GetBlockedProductsEmitter(
		settings.Brokers,
		settings.BlockedProductsTopic,
		blockedProductCodec)
	if err != nil {
		log.Fatalf("Ошибка получения эммитера заблокированных продуктов %v\n", err)
	}

	view, err := GetBlockedProductsView(
		settings.Brokers,
		settings.BlockedProductsGroupTableTopic,
		intSliceCodec)
	if err != nil {
		log.Fatalf("Ошибка получения представления слов %v\n", err)
	}

	g.Go(func() error {
		processor, err := GetBlockedProductsProcessor(
			settings.Brokers,
			settings.BlockedProductsGroup,
			settings.BlockedProductsTopic,
			blockedProductCodec,
			intSliceCodec)

		if err != nil {
			return err
		}

		err = processor.Run(ctx)
		return err
	})

	g.Go(func() error {
		processor, err := GetProductFiltrationProcessor(
			settings.Brokers,
			settings.ProductsConsumerGroup,
			settings.BlockedProductsGroupTableTopic,
			intSliceCodec,
			settings.InputProductsTopic,
			settings.OutputProductsTopic,
			inputProductCodec,
		)

		if err != nil {
			return err
		}

		err = processor.Run(ctx)
		return err
	})

	g.Go(func() error {
		err := view.Run(ctx)
		return err
	})

	handler := NewApiHandler(NewBlockedProductsService(view, emitter))
	server := NewGinServer(handler, "8099")
	go server.ListenAndServe()

	log.Println("Ожидаем завершения")
	err = g.Wait()
	if err != nil {
		log.Fatalf("Ошибка %v\n", err)
	}
	log.Println("Завершено")
}

type Settings struct {
	Brokers                        []string
	SchemaRegistryUrl              string
	SslCaLocation                  string
	Username                       string
	Password                       string
	InputProductsTopic             string
	OutputProductsTopic            string
	BlockedProductsTopic           string
	BlockedProductsGroup           string
	BlockedProductsGroupTableTopic string
	ProductsConsumerGroup          string
}

func getSchemaRegistryConfig(url string) *schemaregistry.Config {
	return schemaregistry.NewConfig(url)
}

func getSettings() Settings {
	servers := getEnv("BOOTSTRAP_SERVERS", "kafka-1:19093,kafka-2:19093,kafka-3:19093")
	brokers := strings.Split(servers, ",")
	return Settings{
		Brokers:                        brokers,
		SchemaRegistryUrl:              getEnv("REGISTRY", "http://schema-registry:8080"),
		SslCaLocation:                  getEnv("SSL_CA_LOCATION", "./ca.crt"),
		Username:                       getEnv("USERNAME", "filtering_app"),
		Password:                       getEnv("PASSWORD", "filtering_app-secret"),
		BlockedProductsGroup:           getEnv("BLOCKED_PRODUCTS_GROUP", "blocked-products-group"),
		ProductsConsumerGroup:          getEnv("PRODUCTS_RAW_GROUP", "products-raw-group"),
		InputProductsTopic:             getEnv("PRODUCTS_RAW_TOPIC", "products-raw"),
		OutputProductsTopic:            getEnv("PRODUCTS_TOPIC", "products"),
		BlockedProductsTopic:           getEnv("BLOCKED_PRODUCTS_TOPIC", "blocked-products"),
		BlockedProductsGroupTableTopic: getEnv("BLOCKED_PRODUCTS_GROUP_TABLE", "blocked-products-group-table"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getSaramaConfig(settings Settings) *sarama.Config {
	config := goka.DefaultConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Net.SASL.Enable = true
	config.Net.SASL.User = settings.Username
	config.Net.SASL.Password = settings.Password
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.TLS.Enable = true

	caCert, err := os.ReadFile(settings.SslCaLocation)
	if err != nil {
		log.Fatalln("Не загрузить сертификат")
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs: caPool,
	}

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	return config
}
