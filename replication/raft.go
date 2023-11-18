package replication

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"time"

	"github.com/asynched/repl/config"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
)

// GetRaft returns a raft instance
func GetRaft(config *config.Config, fsm raft.FSM) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.RaftAddr)

	raftConfig.LogOutput = io.Discard
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 2

	transport, err := raft.NewTCPTransport(config.RaftAddr, nil, 3, 10*time.Second, nil)

	if err != nil {
		return nil, err
	}

	if err := createFolderIfNotExists("data"); err != nil {
		return nil, err
	}

	if err := createFolderIfNotExists("data/logs"); err != nil {
		return nil, err
	}

	if err := createFolderIfNotExists("data/stable"); err != nil {
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

func createFolderIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, fs.ModePerm)
	}

	return nil
}
