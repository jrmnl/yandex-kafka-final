package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ApiHandler struct {
	blockedProducts *BlockedProductsService
}

func NewApiHandler(blockedProducts *BlockedProductsService) *ApiHandler {
	return &ApiHandler{
		blockedProducts: blockedProducts,
	}
}

// (GET /blocked-products)
func (h *ApiHandler) GetBlockedProducts(c *gin.Context) {
	words, err := h.blockedProducts.ListBlockedProducts()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	c.JSON(http.StatusOK, nonNil(words))
}

// (PUT /blocked-products/{productId})
func (h *ApiHandler) BlockProduct(c *gin.Context, productId int) {
	var req string
	if err := c.ShouldBindPlain(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	err := h.blockedProducts.AddBlockedProduct(productId)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.Status(http.StatusNoContent)
}

// (DELETE /blocked-products/{productId})
func (h *ApiHandler) RemoveBlock(c *gin.Context, productId int) {
	err := h.blockedProducts.RemoveBlockedProduct(productId)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	c.Status(http.StatusOK)
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
