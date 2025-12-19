package grpc

import (
	"context"
	"net"
	"sync/atomic"

	logging "github.com/ipfs/go-log/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/celestiaorg/celestia-node/blob"
	nodeblob "github.com/celestiaorg/celestia-node/nodebuilder/blob"
	pb "github.com/celestiaorg/celestia-node/nodebuilder/grpc/pb"
	"github.com/celestiaorg/celestia-node/state"

	libshare "github.com/celestiaorg/go-square/v3/share"
)

var log = logging.Logger("grpc")

// Server is the gRPC server for blob operations.
type Server struct {
	pb.UnimplementedBlobServiceServer

	cfg      *Config
	blobMod  nodeblob.Module
	srv      *grpc.Server
	listener net.Listener
	started  atomic.Bool
}

// NewServer creates a new gRPC server.
func NewServer(cfg *Config, blobMod nodeblob.Module) *Server {
	return &Server{
		cfg:     cfg,
		blobMod: blobMod,
	}
}

// Start starts the gRPC server.
func (s *Server) Start(ctx context.Context) error {
	if !s.cfg.Enabled {
		log.Info("gRPC server disabled")
		return nil
	}

	if !s.started.CompareAndSwap(false, true) {
		log.Warn("gRPC server already started")
		return nil
	}

	addr := net.JoinHostPort(s.cfg.Address, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener

	s.srv = grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64MB
		grpc.MaxSendMsgSize(64*1024*1024), // 64MB
	)

	// Register the blob service
	pb.RegisterBlobServiceServer(s.srv, s)

	// Enable reflection for debugging with grpcurl
	reflection.Register(s.srv)

	log.Infow("gRPC server started", "address", addr)

	go func() {
		if err := s.srv.Serve(listener); err != nil {
			log.Errorw("gRPC server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the gRPC server.
func (s *Server) Stop(ctx context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}

	if !s.started.CompareAndSwap(true, false) {
		log.Warn("gRPC server already stopped")
		return nil
	}

	if s.srv != nil {
		s.srv.GracefulStop()
	}

	log.Info("gRPC server stopped")
	return nil
}

// GetAll implements the gRPC GetAll method.
func (s *Server) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	// Convert namespaces from bytes to libshare.Namespace
	namespaces := make([]libshare.Namespace, len(req.Namespaces))
	for i, ns := range req.Namespaces {
		namespace, err := libshare.NewNamespaceFromBytes(ns)
		if err != nil {
			return nil, err
		}
		namespaces[i] = namespace
	}

	// Call the blob module directly
	blobs, err := s.blobMod.GetAll(ctx, req.Height, namespaces)
	if err != nil {
		return nil, err
	}

	// Convert blobs to protobuf format
	pbBlobs := make([]*pb.Blob, len(blobs))
	for i, b := range blobs {
		pbBlobs[i] = blobToProto(b)
	}

	return &pb.GetAllResponse{
		Blobs: pbBlobs,
	}, nil
}

// Get implements the gRPC Get method.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	namespace, err := libshare.NewNamespaceFromBytes(req.Namespace)
	if err != nil {
		return nil, err
	}

	b, err := s.blobMod.Get(ctx, req.Height, namespace, blob.Commitment(req.Commitment))
	if err != nil {
		return nil, err
	}

	return &pb.GetResponse{
		Blob: blobToProto(b),
	}, nil
}

// GetProof implements the gRPC GetProof method.
func (s *Server) GetProof(ctx context.Context, req *pb.GetProofRequest) (*pb.GetProofResponse, error) {
	namespace, err := libshare.NewNamespaceFromBytes(req.Namespace)
	if err != nil {
		return nil, err
	}

	proof, err := s.blobMod.GetProof(ctx, req.Height, namespace, blob.Commitment(req.Commitment))
	if err != nil {
		return nil, err
	}

	return &pb.GetProofResponse{
		Proof: proofToProto(proof),
	}, nil
}

// Submit implements the gRPC Submit method.
func (s *Server) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	// Convert protobuf blobs to internal blobs
	blobs := make([]*blob.Blob, len(req.Blobs))
	for i, pbBlob := range req.Blobs {
		namespace, err := libshare.NewNamespaceFromBytes(pbBlob.Namespace)
		if err != nil {
			return nil, err
		}

		var b *blob.Blob
		if len(pbBlob.Signer) > 0 {
			b, err = blob.NewBlobV1(namespace, pbBlob.Data, pbBlob.Signer)
		} else {
			b, err = blob.NewBlobV0(namespace, pbBlob.Data)
		}
		if err != nil {
			return nil, err
		}
		blobs[i] = b
	}

	// Create submit options using functional options
	var configOpts []state.ConfigOption
	if req.GasPrice > 0 {
		configOpts = append(configOpts, state.WithGasPrice(req.GasPrice))
	}
	opts := state.NewTxConfig(configOpts...)

	height, err := s.blobMod.Submit(ctx, blobs, opts)
	if err != nil {
		return nil, err
	}

	return &pb.SubmitResponse{
		Height: height,
	}, nil
}

// blobToProto converts an internal Blob to protobuf format.
func blobToProto(b *blob.Blob) *pb.Blob {
	if b == nil {
		return nil
	}
	return &pb.Blob{
		Namespace:    b.Namespace().Bytes(),
		Data:         b.Data(),
		Commitment:   b.Commitment,
		ShareVersion: uint32(b.ShareVersion()),
		Index:        uint64(b.Index()),
		Signer:       b.Signer(),
	}
}

// proofToProto converts an internal Proof to protobuf format.
func proofToProto(p *blob.Proof) *pb.Proof {
	if p == nil {
		return nil
	}

	nmtProofs := make([]*pb.NMTProof, len(*p))
	for i, proof := range *p {
		nmtProofs[i] = &pb.NMTProof{
			Start:                 int64(proof.Start()),
			End:                   int64(proof.End()),
			Nodes:                 proof.Nodes(),
			LeafHash:              proof.LeafHash(),
			IsMaxNamespaceIgnored: proof.IsMaxNamespaceIDIgnored(),
		}
	}

	return &pb.Proof{
		Proofs: nmtProofs,
	}
}
