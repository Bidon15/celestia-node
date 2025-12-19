// Package grpc provides a high-level gRPC client for Celestia blob operations.
// This is the recommended client for sequencers and high-performance applications.
package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/celestiaorg/celestia-node/nodebuilder/grpc/pb"
)

// Client is a high-level gRPC client for Celestia blob operations.
// It provides a simple interface for both reading and writing blobs.
type Client struct {
	conn   *grpc.ClientConn
	client pb.BlobServiceClient
}

// Blob represents a data blob in Celestia.
type Blob struct {
	Namespace    []byte
	Data         []byte
	Commitment   []byte
	ShareVersion uint32
	Index        uint64
	Signer       []byte
}

// SubmitOptions configures blob submission.
type SubmitOptions struct {
	GasPrice float64
}

// NewClient creates a new gRPC client connected to the given address.
//
// Example:
//
//	client, err := grpc.NewClient("localhost:9091")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
func NewClient(address string, opts ...grpc.DialOption) (*Client, error) {
	// Default options
	defaultOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024), // 64MB
			grpc.MaxCallSendMsgSize(64*1024*1024), // 64MB
		),
	}

	// Merge with user options (user opts take precedence)
	allOpts := append(defaultOpts, opts...)

	conn, err := grpc.NewClient(address, allOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewBlobServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// GetAll retrieves all blobs for the given namespaces at a specific height.
// This is the primary read operation for sequencers.
//
// Example:
//
//	blobs, err := client.GetAll(ctx, 12345, [][]byte{myNamespace})
func (c *Client) GetAll(ctx context.Context, height uint64, namespaces [][]byte) ([]*Blob, error) {
	resp, err := c.client.GetAll(ctx, &pb.GetAllRequest{
		Height:     height,
		Namespaces: namespaces,
	})
	if err != nil {
		return nil, err
	}

	return pbBlobsToBlobs(resp.Blobs), nil
}

// Get retrieves a specific blob by namespace and commitment.
//
// Example:
//
//	blob, err := client.Get(ctx, 12345, namespace, commitment)
func (c *Client) Get(ctx context.Context, height uint64, namespace, commitment []byte) (*Blob, error) {
	resp, err := c.client.Get(ctx, &pb.GetRequest{
		Height:     height,
		Namespace:  namespace,
		Commitment: commitment,
	})
	if err != nil {
		return nil, err
	}

	if resp.Blob == nil {
		return nil, nil
	}

	return pbBlobToBlob(resp.Blob), nil
}

// Submit submits blobs to Celestia and returns the height at which they were included.
// This is the primary write operation for sequencers.
//
// Example:
//
//	height, err := client.Submit(ctx, []*grpc.Blob{
//	    {Namespace: ns, Data: myData},
//	}, nil)
func (c *Client) Submit(ctx context.Context, blobs []*Blob, opts *SubmitOptions) (uint64, error) {
	pbBlobs := make([]*pb.Blob, len(blobs))
	for i, b := range blobs {
		pbBlobs[i] = &pb.Blob{
			Namespace:    b.Namespace,
			Data:         b.Data,
			ShareVersion: b.ShareVersion,
			Signer:       b.Signer,
		}
	}

	req := &pb.SubmitRequest{
		Blobs: pbBlobs,
	}
	if opts != nil && opts.GasPrice > 0 {
		req.GasPrice = opts.GasPrice
	}

	resp, err := c.client.Submit(ctx, req)
	if err != nil {
		return 0, err
	}

	return resp.Height, nil
}

// SubmitAndWait submits blobs and waits for confirmation by reading them back.
// Returns the height and the blobs with their commitments populated.
func (c *Client) SubmitAndWait(ctx context.Context, blobs []*Blob, opts *SubmitOptions) (uint64, []*Blob, error) {
	height, err := c.Submit(ctx, blobs, opts)
	if err != nil {
		return 0, nil, err
	}

	// Extract namespaces for read-back
	nsMap := make(map[string]bool)
	for _, b := range blobs {
		nsMap[string(b.Namespace)] = true
	}
	namespaces := make([][]byte, 0, len(nsMap))
	for ns := range nsMap {
		namespaces = append(namespaces, []byte(ns))
	}

	// Read back to get commitments
	readBlobs, err := c.GetAll(ctx, height, namespaces)
	if err != nil {
		return height, nil, fmt.Errorf("submitted at height %d but failed to read back: %w", height, err)
	}

	return height, readBlobs, nil
}

// WatchBlobs streams new blobs for the given namespace starting from startHeight.
// This is useful for sequencers that need to react to new data.
//
// The callback is invoked for each height that has blobs.
// Return an error from the callback to stop watching.
func (c *Client) WatchBlobs(
	ctx context.Context,
	namespace []byte,
	startHeight uint64,
	pollInterval time.Duration,
	callback func(height uint64, blobs []*Blob) error,
) error {
	currentHeight := startHeight

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			blobs, err := c.GetAll(ctx, currentHeight, [][]byte{namespace})
			if err != nil {
				// Height might not exist yet, continue polling
				continue
			}

			if len(blobs) > 0 {
				if err := callback(currentHeight, blobs); err != nil {
					return err
				}
			}

			currentHeight++
		}
	}
}

// Helper conversions
func pbBlobToBlob(pb *pb.Blob) *Blob {
	if pb == nil {
		return nil
	}
	return &Blob{
		Namespace:    pb.Namespace,
		Data:         pb.Data,
		Commitment:   pb.Commitment,
		ShareVersion: pb.ShareVersion,
		Index:        pb.Index,
		Signer:       pb.Signer,
	}
}

func pbBlobsToBlobs(pbs []*pb.Blob) []*Blob {
	blobs := make([]*Blob, len(pbs))
	for i, pb := range pbs {
		blobs[i] = pbBlobToBlob(pb)
	}
	return blobs
}
