package main

import (
	"flag"
	"log"

	"github.com/asynched/repl/config"
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

var (
	filename = flag.String("config", "config.toml", "path to config file")
)

const DEBUG bool = true

func main() {
	flag.Parse()

	if *filename == "" {
		log.Fatal("Error: config file is required")
	}

	config, err := config.ParseConfig(*filename)

	if err != nil {
		log.Fatalf("Error parsing config file: %v\n", err)
	}

	log.Println("Initializing modules")
	log.Println("Initializing topic manager")

	var topicManager managers.TopicManager

	if !config.Cluster {
		log.Println("Initializing server as standalone node")
		topicManager = managers.NewStandaloneTopicManager()
	} else {
		log.Fatal("Error initializing server: raft replication is not implemented")
	}

	if DEBUG {
		log.Println("Initializing 'demo' topic")
		topicManager.CreateTopic("demo")
	}

	// HTTP server
	log.Println("Initializing http server")
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	healthController := server.NewHealthController()
	healthController.Setup(app.Group("/health"))

	topicController := server.NewTopicController(topicManager)
	topicController.Setup(app.Group("/topics"))

	app.Static("/", "./public")

	log.Printf("Listening on address: http://%s\n", config.HttpAddr)
	log.Printf("Check health status at: http://%s/health\n", config.HttpAddr)
	log.Fatalf("Error starting server: %v\n", app.Listen(config.HttpAddr))
}
