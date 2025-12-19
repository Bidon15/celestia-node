package grpc

import (
	"context"

	"go.uber.org/fx"

	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

// ConstructModule creates the gRPC module.
func ConstructModule(tp node.Type, cfg *Config) fx.Option {
	// Validate config
	cfgErr := cfg.Validate()

	return fx.Module("grpc",
		fx.Supply(cfg),
		fx.Error(cfgErr),
		fx.Provide(fx.Annotate(
			NewServer,
			fx.OnStart(func(ctx context.Context, server *Server) error {
				return server.Start(ctx)
			}),
			fx.OnStop(func(ctx context.Context, server *Server) error {
				return server.Stop(ctx)
			}),
		)),
		// Invoke ensures the server is actually instantiated
		fx.Invoke(func(*Server) {}),
	)
}

