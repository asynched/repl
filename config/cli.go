package config

import (
	"errors"
	"log"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Name      string `toml:"name"`
	HttpAddr  string `toml:"http_addr"`
	RaftAddr  string `toml:"raft_addr"`
	Cluster   bool   `toml:"cluster"`
	Bootstrap bool   `toml:"bootstrap"`
	Peers     []Peer `toml:"peers"`
}

type Peer struct {
	HttpAddr string `toml:"http_addr"`
	RaftAddr string `toml:"raft_addr"`
}

func (config *Config) Validate() error {
	if config.HttpAddr == "" {
		return errors.New("http_addr is required")
	}

	if config.Cluster && config.RaftAddr == "" {
		return errors.New("raft_addr is required")
	}

	if config.Bootstrap && len(config.Peers) == 0 {
		return errors.New("peers are required")
	}

	if !config.Bootstrap && len(config.Peers) > 0 {
		return errors.New("peers are not allowed")
	}

	return nil
}

func ParseConfig(filename string) (*Config, error) {
	if filename == "" {
		log.Println("No config file provided")
		log.Println("Using default settings")

		return DefaultConfig(), nil
	}

	config := &Config{}

	if _, err := toml.DecodeFile(filename, config); err != nil {
		return config, err
	}

	return config, nil
}

func DefaultConfig() *Config {
	return &Config{
		Name:      "repl",
		HttpAddr:  "0.0.0.0:9000",
		RaftAddr:  "",
		Cluster:   false,
		Bootstrap: false,
		Peers:     []Peer{},
	}
}
