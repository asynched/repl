package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

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
	flag.Parse()

	cfg, err := config.ParseConfig(*filename)

	if err != nil {
		log.Fatalf("Error parsing config: %v\n", err)
	}

	if err := profileApplication(); err != nil {
		log.Fatalf("Error initializing profiler: %v\n", err)
	}

	log.Println("Application has started")
	log.Printf("Process id: %d\n", os.Getpid())

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

	app.Static("/", "./public")

	log.Printf("HTTP server listening on address: http://%s\n", cfg.HttpAddr)

	if cfg.Cluster {
		log.Printf("Raft server listening on address: %s\n", cfg.RaftAddr)
	}

	go func() {
		time.Sleep(5 * time.Second)
		mem := runtime.MemStats{}

		for {
			routines := runtime.NumGoroutine()

			runtime.ReadMemStats(&mem)

			heap := mem.HeapAlloc / 1024 / 1024
			alloc := mem.Alloc / 1024 / 1024
			stack := mem.StackInuse / 1024 / 1024

			log.Printf("type='stats' routines=%d heap=%d alloc=%d stack=%d\n", routines, heap, alloc, stack)
			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf("Check health status at: http://%s/health\n", cfg.HttpAddr)
	log.Fatalf("Error starting server: %v\n", app.Listen(cfg.HttpAddr))
}

func profileApplication() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	cpu, err := os.OpenFile("cpu.prof", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	heap, err := os.OpenFile("heap.prof", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	if err := pprof.StartCPUProfile(cpu); err != nil {
		return err
	}

	go func() {
		<-c

		pprof.StopCPUProfile()

		if err := pprof.WriteHeapProfile(heap); err != nil {
			log.Fatalf("Error writing heap profile: %v\n", err)
		}

		cpu.Close()
		heap.Close()

		log.Println("Profiling has stopped")
		log.Println("Application has stopped")

		os.Exit(0)
	}()

	return nil
}
