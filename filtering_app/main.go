package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

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
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	log.Println("Ожидаем создание топиков")
	// WaitTopics(ctx)
	log.Println("Ожидание топиков завершено")

	g, ctx := errgroup.WithContext(ctx)

	cfg := goka.DefaultConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(cfg)

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
	/*
		outpurProductCodec := codecs.NewAvroSerializer[Product](
		registryConfig,
		settings.OutputProductsTopic)
	*/

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

		return processor.Run(ctx)
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

		return processor.Run(ctx)
	})

	g.Go(func() error {
		return view.Run(ctx)
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

/*
// Ждет создание топика в кафке
func WaitTopics(ctx context.Context) {
	for {
		time.Sleep(2 * time.Second)
		select {
		case <-ctx.Done():
			return
		default:
			admin, err := sarama.NewClusterAdmin(Brokers, sarama.NewConfig())
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
				expectedTopics := []string{
					"messages",
					"filtered_messages",
					"blocked_users",
					"blocked_users-group-table",
					"blocked_words",
					"blocked_words-group-table",
				}
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
*/

type Settings struct {
	Brokers                        []string
	SchemaRegistryUrl              string
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
	return Settings{
		Brokers:                        []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"},
		SchemaRegistryUrl:              "http://schema-registry:8080",
		InputProductsTopic:             "products-raw",
		OutputProductsTopic:            "products",
		BlockedProductsTopic:           "blocked-products",
		BlockedProductsGroup:           "blocked-products-group",
		BlockedProductsGroupTableTopic: "blocked-products-group-table",
		ProductsConsumerGroup:          "products-raw-group",
	}
}
