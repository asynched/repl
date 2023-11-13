package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/asynched/repl/domain/entities"
	"github.com/asynched/repl/managers"
	"github.com/gofiber/fiber/v2"
)

type TopicController struct {
	manager managers.TopicManager
}

func (controller *TopicController) HandleGetTopics(c *fiber.Ctx) error {
	return c.JSON(controller.manager.GetTopics())
}

func (controller *TopicController) HandleCreateTopic(c *fiber.Ctx) error {
	var data struct {
		Name string `json:"name"`
	}

	if err := c.BodyParser(&data); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
			"cause": err.Error(),
		})
	}

	if err := controller.manager.CreateTopic(data.Name); err != nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": "topic already exists",
			"cause": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "topic created",
	})
}

func (controller *TopicController) HandlePublishMessage(c *fiber.Ctx) error {
	topicName := c.Params("topic")

	var data struct {
		Value   string            `json:"value"`
		Headers map[string]string `json:"headers"`
	}

	if err := c.BodyParser(&data); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
			"cause": err.Error(),
		})
	}

	message := &entities.Message{
		Value:   data.Value,
		Headers: data.Headers,
	}

	if err := controller.manager.PublishMessage(topicName, message); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to publish message",
			"cause": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "message published",
		"data":    message,
	})
}

func (controller *TopicController) HandleSSE(c *fiber.Ctx) error {
	topicName := c.Params("topic")
	clientAddress := c.Context().RemoteAddr()

	if !controller.manager.Exists(topicName) {
		log.Printf("Failed to connect client '%s' to topic '%s': topic not found\n", clientAddress, topicName)

		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "topic not found",
		})
	}

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	log.Printf("'%s' has subscribed to topic '%s'\n", clientAddress, topicName)

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer func() {
			log.Printf("'%s' disconnected from topic '%s'\n", clientAddress, topicName)
		}()

		channel, err := controller.manager.Subscribe(topicName)

		if err != nil {
			log.Printf("Failed to connect client '%s' to topic '%s': %v\n", clientAddress, topicName, err)
		}

		defer controller.manager.Unsubscribe(topicName, channel)

		// Sends a connection event to the client
		if _, err := fmt.Fprintf(w, "event: %s\n\n", "connected"); err != nil {
			return
		}

		// Flushes the buffer to the client
		if err := w.Flush(); err != nil {
			return
		}

		for {
			select {
			case message := <-channel:
				data, _ := json.Marshal(message)

				sent, err := fmt.Fprintf(w, "data: %s\n\n", data)

				if err != nil {
					return
				}

				if sent == 0 {
					return
				}

				if err := w.Flush(); err != nil {
					return
				}
			case <-time.After(5 * time.Second):
				log.Printf("Sending ping event to '%s' (5s timeout)\n", clientAddress)

				sent, err := fmt.Fprint(w, "event: ping\n\n")

				log.Printf("Sent=%d\tError=%v\n", sent, err)

				if err != nil {
					return
				}

				if sent == 0 {
					return
				}

				if err := w.Flush(); err != nil {
					return
				}
			}
		}
	})

	return nil
}

func (controller *TopicController) Setup(router fiber.Router) {
	router.Get("/", controller.HandleGetTopics)
	router.Post("/", controller.HandleCreateTopic)
	router.Post("/:topic", controller.HandlePublishMessage)
	router.Get("/:topic/sse", controller.HandleSSE)
}

func NewTopicController(manager managers.TopicManager) *TopicController {
	return &TopicController{
		manager: manager,
	}
}
