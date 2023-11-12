package main

import (
	"log"

	"github.com/asynched/repl/managers"
	"github.com/asynched/repl/server"
	"github.com/gofiber/fiber/v2"
)

func init() {
	log.SetFlags(0)

	log.Println(`                    ___      
                   /\_ \     
 _ __    __   _____\//\ \    
/\  __\/'__'\/\  __ \\ \ \   
\ \ \//\  __/\ \ \_\ \\_\ \_ 
 \ \_\\ \____\\ \  __//\____\
  \/_/ \/____/ \ \ \/ \/____/
                \ \_\        
                 \/_/`)
	log.Println()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.Lmsgprefix)
	log.SetPrefix("[repl] ")
}

func main() {
	serverAddress := "127.0.0.1:9001"

	log.Println("Initializing modules")

	log.Println("Initializing topic manager")
	topicManager := managers.NewTopicManager()

	topicManager.CreateTopic("demo")

	// HTTP server
	log.Println("Initializing http server")
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	healthController := server.NewHealthController()
	app.Get("/health", healthController.GetHealth)

	topicController := server.NewTopicController(topicManager)
	app.Get("/topics", topicController.HandleGetTopics)
	app.Post("/topics", topicController.HandleCreateTopic)

	app.Get("/topics/:topic/sse", topicController.HandleSSE)
	app.Post("/topics/:topic", topicController.HandlePublishMessage)

	// TODO: Remove this
	app.Static("/", "./public")

	log.Printf("Listening on address: http://%s\n", "127.0.0.1:9001")
	log.Printf("Check health status at: http://%s/health\n", serverAddress)

	log.Fatalf("Error starting server: %v", app.Listen(serverAddress))
}
