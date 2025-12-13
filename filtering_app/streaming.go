package main

import (
	"log"
	"slices"
	"strconv"

	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/codecs"
	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/events"
	"github.com/lovoo/goka"
)

func GetBlockedProductsEmitter(
	brokers []string,
	topic string,
	codec *codecs.Avro[events.BlockedProduct]) (*goka.Emitter, error) {

	return goka.NewEmitter(brokers, goka.Stream(topic), codec)
}

func GetBlockedProductsView(
	brokers []string,
	topic string,
	codec *codecs.Avro[[]int]) (*goka.View, error) {

	return goka.NewView(brokers, goka.Table(topic), codec)
}

func GetBlockedProductsProcessor(
	brokers []string,
	groupName string,
	inputTopic string,
	streamCodec *codecs.Avro[events.BlockedProduct],
	tableCodec *codecs.Avro[[]int]) (*goka.Processor, error) {

	g := goka.DefineGroup(
		goka.Group(groupName),
		goka.Persist(tableCodec),
		goka.Input(
			goka.Stream(inputTopic),
			streamCodec,
			func(ctx goka.Context, msg any) {
				if event, ok := msg.(events.BlockedProduct); ok {
					var blocked []int
					if currentlyBlocked, ok := ctx.Value().([]int); ok {
						blocked = currentlyBlocked
					} else {
						blocked = make([]int, 0)
					}

					switch event.Action {
					case events.Add:
						if !slices.Contains(blocked, event.ProductId) {
							blocked = append(blocked, event.ProductId)
							ctx.SetValue(blocked)
						}
					case events.Remove:
						for i, id := range blocked {
							if id == event.ProductId {
								blocked = slices.Delete(blocked, i, i+1)
								ctx.SetValue(blocked)
								break
							}
						}
					}
				}
			}),
	)

	return goka.NewProcessor(brokers, g)
}

func GetProductFiltrationProcessor(
	brokers []string,
	groupName string,
	blockedProductsTable string,
	blockedProductsTableCodec *codecs.Avro[[]int],
	inputTopic string,
	outputTopic string,
	productCodec *codecs.Avro[Product]) (*goka.Processor, error) {

	g := goka.DefineGroup(
		goka.Group(groupName),
		goka.Lookup(goka.Table(blockedProductsTable), blockedProductsTableCodec),
		goka.Output(goka.Stream(outputTopic), productCodec),
		goka.Input(
			goka.Stream(inputTopic),
			productCodec,
			func(ctx goka.Context, msg any) {
				productId, err := strconv.Atoi(ctx.Key())
				if err != nil {
					log.Printf(
						"Ошибка десериализации ключа (%s): %v",
						ctx.Key(),
						err)
					return
				}

				blockedProductsA := ctx.Lookup(goka.Table(blockedProductsTable), BLOCKED_PRODUCT_KEY)
				if blockedProducts, ok := blockedProductsA.([]int); ok {
					if slices.Contains(blockedProducts, productId) {
						log.Printf("Продукт заблокирован: %s", ctx.Key())
						return
					}
				}

				ctx.Emit(goka.Stream(outputTopic), ctx.Key(), msg)
			}),
	)

	return goka.NewProcessor(brokers, g)
}
