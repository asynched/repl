package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/asynched/repl/config"
	"github.com/asynched/repl/managers"
	"github.com/asynched/repl/replication"
	"github.com/asynched/repl/server"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
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
	filename = flag.String("config", "", "path to config file")
)

func main() {
	cpu, err := os.OpenFile("repl-cpu.prof", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	heap, err := os.OpenFile("repl-heap.prof", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	err = pprof.StartCPUProfile(cpu)
	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 2)

	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		for range c {
			log.Println("Stopping profiler")
			pprof.StopCPUProfile()
			pprof.WriteHeapProfile(heap)
			cpu.Close()
			os.Exit(0)
		}
	}()

	flag.Parse()

	cfg, err := config.ParseConfig(*filename)

	if err != nil {
		log.Fatalf("Error parsing config: %v\n", err)
	}

	log.Println("Initializing modules")
	log.Println("Initializing topic manager")

	var topicManager managers.TopicManager = managers.NewStandaloneTopicManager()

	if cfg.Cluster {
		log.Println("Initializing server as a cluster")
		log.Println("Initializing raft")
		manager := managers.NewRaftTopicManager()

		if cfg.Bootstrap {
			log.Println("Node is bootstrapping cluster")
		}

		raft, err := replication.GetRaft(cfg, manager)

		if err != nil {
			log.Fatalf("Error initializing raft: %v\n", err)
		}

		log.Println("Raft successfully initialized")
		manager.Configure(raft)
		topicManager = manager
	} else {
		topicManager.CreateTopic("demo")
		log.Println("Initializing server as standalone node")
	}

	log.Println("Initializing HTTP server")

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Use(cors.New())

	healthController := server.NewHealthController()
	healthController.Setup(app.Group("/health"))

	topicController := server.NewTopicController(topicManager)
	topicController.Setup(app.Group("/topics"))

	log.Printf("HTTP server listening on address: http://%s\n", cfg.HttpAddr)

	if cfg.Cluster {
		log.Printf("Raft server listening on address: %s\n", cfg.RaftAddr)
	}

	log.Printf("Check health status at: http://%s/health\n", cfg.HttpAddr)
	log.Fatalf("Error starting server: %v\n", app.Listen(cfg.HttpAddr))
}
