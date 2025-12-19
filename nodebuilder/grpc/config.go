package grpc

import (
	"fmt"
)

// Config contains configuration for the gRPC server.
type Config struct {
	// Enabled controls whether the gRPC server is started.
	Enabled bool
	// Address is the bind address for the gRPC server.
	Address string
	// Port is the port for the gRPC server.
	Port string
}

// DefaultConfig returns the default gRPC configuration.
func DefaultConfig() Config {
	return Config{
		Enabled: false, // Disabled by default, opt-in for now
		Address: "0.0.0.0",
		Port:    "9091",
	}
}

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if cfg.Port == "" {
		return fmt.Errorf("grpc port cannot be empty")
	}
	return nil
}

