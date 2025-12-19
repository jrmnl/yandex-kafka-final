package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-gonic/gin"
	"github.com/jrmnl/yandex-kafka-final-client-app/api"
)

type ApiHandler struct {
	es      *elasticsearch.Client
	actions chan UserAction
}

func NewApiHandler(es *elasticsearch.Client, actions chan UserAction) *ApiHandler {
	return &ApiHandler{es, actions}
}

// (GET /product)
func (h *ApiHandler) GetProductByName(c *gin.Context, params api.GetProductByNameParams) {
	res, err := h.query(c.Request.Context(), params.Name)
	h.actions <- UserAction{
		UserId:              params.XUserId,
		SearchedProductName: params.Name,
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}
	c.JSON(http.StatusOK, res)
}

func (h *ApiHandler) query(ctx context.Context, name string) (*[]Product, error) {
	query := fmt.Sprintf(`{ "query": { "match": { "name": "%s"} } }`, name)
	res, err := h.es.Search(
		h.es.Search.WithContext(ctx),
		h.es.Search.WithIndex("products"),
		h.es.Search.WithBody(strings.NewReader(query)),
	)

	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	var esResp ElasticResponse
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return nil, err
	}

	ids := make([]Product, 0, len(esResp.Hits.Hits))
	for _, p := range esResp.Hits.Hits {
		ids = append(ids, p.Source)
	}
	return &ids, nil
}

type ElasticResponse struct {
	Hits Hits `json:"hits"`
}

type Hits struct {
	Hits []Hit `json:"hits"`
}

type Hit struct {
	Source Product `json:"_source"`
}
