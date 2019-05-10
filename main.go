package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mercadolibre/Elasticsearch-Go/Domain"
	"github.com/olivere/elastic"
)
const (
	elasticIndexName = "itemdata"
	elasticTypeName  = "item"
)

var (
	elasticClient *elastic.Client
)

func main(){

	var err error
	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://localhost:9200"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}
	r := gin.Default()
	r.POST("/item", createItemEndpoint)
	r.GET("/item/:Id", getItemEndPoint)
	r.GET("/search/items", searchEndpoint)
	if err = r.Run(":9220"); err != nil {
		log.Fatal(err)
	}
}

func getItemEndPoint(c *gin.Context) {
	getItem, err := elasticClient.Get().
		Index(elasticIndexName).
		Type(elasticTypeName).
		Id(c.Param("Id")).
		Do(c)

	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Item not found")
		return
	}
		c.JSON(http.StatusAccepted, getItem.Source)
		return
}

func createItemEndpoint(c *gin.Context) {

	var item Domain.Item
	if err := c.BindJSON(&item); err != nil {
		errorResponse(c, http.StatusBadRequest, "Malformed request body")
		return
	}
	bulk := elasticClient.
		Bulk().
		Index(elasticIndexName).
		Type(elasticTypeName)

		bulk.Add(elastic.NewBulkIndexRequest().Id(item.Id).Doc(item))

	if _, err := bulk.Do(c.Request.Context()); err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Failed to create items")
		return
	}
	c.Status(http.StatusOK)
}

func searchEndpoint(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		errorResponse(c, http.StatusBadRequest, "Query not specified")
		return
	}
	skip := 0
	take := 10
	if i, err := strconv.Atoi(c.Query("skip")); err == nil {
		skip = i
	}
	if i, err := strconv.Atoi(c.Query("take")); err == nil {
		take = i
	}
	// Perform search
	esQuery := elastic.NewMultiMatchQuery(query, "title", "content").
		Fuzziness("2").
		MinimumShouldMatch("2")
	result, err := elasticClient.Search().
		Index(elasticIndexName).
		Query(esQuery).
		From(skip).Size(take).
		Do(c.Request.Context())
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Something went wrong")
		return
	}

	c.JSON(http.StatusOK, result.Hits.Hits)
}

func errorResponse(c *gin.Context, code int, err string) {
	c.JSON(code, gin.H{
		"error": err,
	})
}


