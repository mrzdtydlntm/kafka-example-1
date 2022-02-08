package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	_ "github.com/joho/godotenv/autoload"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"

	kafkaexample "github.com/friendsofgo/kafka-example/pkg"
	"github.com/friendsofgo/kafka-example/pkg/kafka"
)

type Request struct {
	Username string `json:"username"`
	Message  string `json:"message"`
}

func main() {
	e := echo.New()
	e.POST("/join", joinHandler)
	e.POST("/publish", publishHandler)
	middleware.Logger()
	e.Start(":8000")
}

func joinHandler(c echo.Context) error {
	var (
		brokers = os.Getenv("KAFKA_BROKERS")
		topic   = os.Getenv("KAFKA_TOPIC")
	)
	publisher := kafka.NewPublisher(strings.Split(brokers, ","), topic)
	var req Request
	err := json.NewDecoder(c.Request().Body).Decode(&req)
	defer c.Request().Body.Close()
	if err != nil {
		c.JSON(http.StatusInternalServerError, "hehe")
	}

	message := kafkaexample.NewSystemMessage(fmt.Sprintf("%s has joined the room!", req.Username))

	if err := publisher.Publish(context.Background(), message); err != nil {
		c.JSON(http.StatusInternalServerError, "hehe")
	}
	return c.JSON(http.StatusAccepted, "message published")
}

func publishHandler(c echo.Context) error {
	var (
		brokers = os.Getenv("KAFKA_BROKERS")
		topic   = os.Getenv("KAFKA_TOPIC")
	)
	publisher := kafka.NewPublisher(strings.Split(brokers, ","), topic)
	var req Request
	err := json.NewDecoder(c.Request().Body).Decode(&req)
	defer c.Request().Body.Close()
	fmt.Println(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "hehe")
	}

	message := kafkaexample.NewMessage(req.Username, req.Message)

	if err := publisher.Publish(context.Background(), message); err != nil {
		c.JSON(http.StatusInternalServerError, "hehe")
	}
	return c.JSON(http.StatusAccepted, "Message Published")
}
