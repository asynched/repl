package config

import (
	"errors"

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
	config := &Config{}

	if _, err := toml.DecodeFile(filename, config); err != nil {
		return nil, err
	}

	return config, nil
}
