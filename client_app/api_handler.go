package main

import (
	"bytes"
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
func (h *ApiHandler) GetProductsByName(c *gin.Context, params api.GetProductsByNameParams) {
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

// (GET /recommendations)
func (h *ApiHandler) GetRecommendations(c *gin.Context, params api.GetRecommendationsParams) {
	query := fmt.Sprintf(`
	{
    	"ksql" :"SELECT PRODUCTS FROM USERS_RECOMENDATIONS WHERE user_id = '%s';"
	}`,
		params.XUserId)

	url := "http://ksqldb-server:8088/query"
	resp, err := http.Post(
		url,
		"application/json",
		bytes.NewBufferString(query),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}
	defer resp.Body.Close()

	var response []KsqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(response) > 1 {
		er := response[1].Row.Columns[0]
		c.JSON(http.StatusOK, er)
	} else {
		c.JSON(http.StatusNotFound, "рекомендации не найдены")
	}
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

type KsqlResponse struct {
	Row KsqlRow `json:"row"`
}

type KsqlRow struct {
	Columns [][]string `json:"columns"`
}

/*
type KsqlResponse struct {

row (object): A single row being returned. This will be null if an error is being returned.

row.columns (array): The values of the columns requested. The schema of the columns was already supplied in header.schema.

row.tombstone (boolean): Whether the row is a deletion of a previous row. It is recommended that you include all columns within the primary key in the projection so that you can determine which previous row was deleted.
*/
