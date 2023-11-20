package server

import (
	"bufio"
	"encoding/json"
	"log"
	"time"

	"github.com/asynched/repl/domain/entities"
	"github.com/asynched/repl/managers"
	"github.com/gofiber/fiber/v2"
)

const (
	eventConnected string = "event: connected\n\n"
	eventPing      string = "event: ping\n\n"
)

const (
	pingInterval time.Duration = 5 * time.Second
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

	if !controller.manager.Exists(topicName) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "topic not found",
		})
	}

	message := entities.Message{
		Value:   data.Value,
		Headers: data.Headers,
	}

	message.FillMissingFields()
	sanitized, _ := json.Marshal(message)

	controller.manager.PublishMessage(topicName, string(sanitized))

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "message published",
		"data":    message,
	})
}

func (controller *TopicController) HandleSSE(c *fiber.Ctx) error {
	topicName := c.Params("topic")
	clientAddress := c.Context().RemoteAddr()
	done := c.Context().Done()

	if !controller.manager.Exists(topicName) {
		log.Printf("Failed to connect client '%s' to topic '%s': topic not found\n", clientAddress, topicName)

		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "topic not found",
		})
	}

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	log.Printf("event='connection' address='%s'\n", clientAddress)

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer log.Printf("event='disconnection' address='%s'\n", clientAddress)

		channel, err := controller.manager.Subscribe(topicName)

		if err != nil {
			log.Printf("event='err_could_not_connect' address='%s' error='%v'\n", clientAddress, err)
			return
		}

		defer controller.manager.Unsubscribe(topicName, channel)

		// Sends a connection event to the client
		if _, err := w.WriteString(eventConnected); err != nil {
			return
		}

		// Flushes the buffer to the client
		if err := w.Flush(); err != nil {
			return
		}

		timer := time.NewTimer(pingInterval)

		for {
			select {
			case messages := <-channel:
				for _, message := range messages {
					if _, err := w.WriteString("data: " + message + "\n\n"); err != nil {
						return
					}
				}

				if err := w.Flush(); err != nil {
					return
				}

				timer.Reset(pingInterval)
			case <-timer.C:
				if _, err := w.WriteString(eventPing); err != nil {
					return
				}

				if err := w.Flush(); err != nil {
					return
				}

				timer.Reset(pingInterval)
			case <-done:
				log.Printf("event='done' client='%s'\n", clientAddress)
				return
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
