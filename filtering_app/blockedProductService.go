package main

import (
	"errors"

	"github.com/jrmnl/yandex-kafka-final-fuiltering-app/events"
	"github.com/lovoo/goka"
)

const BLOCKED_PRODUCT_KEY string = "blocked_products"

type BlockedProductsService struct {
	view    *goka.View
	emitter *goka.Emitter
}

func NewBlockedProductsService(view *goka.View, emitter *goka.Emitter) *BlockedProductsService {
	return &BlockedProductsService{view, emitter}
}

func (s *BlockedProductsService) AddBlockedProduct(productId int) error {
	e := events.BlockedProduct{
		ProductId: productId,
		Action:    events.Add,
	}
	return s.emitter.EmitSync(BLOCKED_PRODUCT_KEY, e)
}

func (s *BlockedProductsService) RemoveBlockedProduct(productId int) error {
	e := events.BlockedProduct{
		ProductId: productId,
		Action:    events.Remove,
	}
	return s.emitter.EmitSync(BLOCKED_PRODUCT_KEY, e)
}

func (s *BlockedProductsService) ListBlockedProducts() ([]int, error) {
	v, err := s.view.Get(BLOCKED_PRODUCT_KEY)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	list, ok := v.([]int)
	if !ok {
		return nil, errors.New("неправильный формат хранения данных в таблице")
	}

	return list, nil
}
