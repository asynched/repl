package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/asynched/repl/config"
	"github.com/asynched/repl/managers"
	"github.com/asynched/repl/server"
	"github.com/gofiber/fiber/v2"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
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
		log.Println("Initializing server as a cluster")
		manager := managers.NewRaftTopicManager()

		log.Println("Initializing raft")

		if config.Bootstrap {
			log.Println("Node is bootstrapping")
		}

		raft, err := getRaft(config, manager)

		if err != nil {
			log.Fatalf("Error initializing raft: %v\n", err)
		}

		log.Println("Raft successfully initialized")
		manager.Configure(raft)
		topicManager = manager
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

func getRaft(config *config.Config, fsm raft.FSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.RaftAddr)

	raftConfig.LogOutput = io.Discard
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 2

	transport, err := raft.NewTCPTransport(config.RaftAddr, nil, 3, 10*time.Second, nil)

	if err != nil {
		return nil, err
	}

	logStore, err := boltdb.NewBoltStore(fmt.Sprintf("data/logs/%s.db", config.Name))

	if err != nil {
		return nil, err
	}

	stableStore, err := boltdb.NewBoltStore(fmt.Sprintf("data/stable/%s.db", config.Name))

	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore("data", 3, nil)

	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)

	if err != nil {
		return nil, err
	}

	if config.Bootstrap {
		servers := make([]raft.Server, len(config.Peers))

		for i, peer := range config.Peers {
			servers[i] = raft.Server{
				ID:      raft.ServerID(peer.RaftAddr),
				Address: raft.ServerAddress(peer.RaftAddr),
			}
		}

		servers = append(servers, raft.Server{
			ID:      raft.ServerID(config.RaftAddr),
			Address: raft.ServerAddress(config.RaftAddr),
		})

		configuration := raft.Configuration{
			Servers: servers,
		}

		future := r.BootstrapCluster(configuration)

		if err := future.Error(); err != nil {
			log.Println("Error bootstrapping cluster, cluster is already bootstrapped")
		}
	}

	return r, nil
}
