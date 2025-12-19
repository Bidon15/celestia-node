package grpc_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	grpcclient "github.com/celestiaorg/celestia-node/api/client/grpc"
)

func Example_sequencerReadLoop() {
	// Connect to your hosted RPC service
	client, err := grpcclient.NewClient("your-rpc.example.com:9091")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Your namespace (29 bytes for v0)
	namespace, _ := hex.DecodeString("00000000000000000000000000000000000000736f762d6e696b6f2d62")

	ctx := context.Background()

	// Read blobs at a specific height
	blobs, err := client.GetAll(ctx, 9286352, [][]byte{namespace})
	if err != nil {
		log.Fatal(err)
	}

	for _, blob := range blobs {
		fmt.Printf("Got blob: %d bytes, commitment: %x\n", len(blob.Data), blob.Commitment[:8])
	}
}

func Example_sequencerSubmit() {
	client, err := grpcclient.NewClient("your-rpc.example.com:9091")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	namespace, _ := hex.DecodeString("00000000000000000000000000000000000000736f762d6e696b6f2d62")

	ctx := context.Background()

	// Submit a blob
	height, err := client.Submit(ctx, []*grpcclient.Blob{
		{
			Namespace: namespace,
			Data:      []byte("hello from my sequencer"),
		},
	}, nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Blob included at height %d\n", height)
}

func Example_sequencerWatch() {
	client, err := grpcclient.NewClient("your-rpc.example.com:9091")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	namespace, _ := hex.DecodeString("00000000000000000000000000000000000000736f762d6e696b6f2d62")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Watch for new blobs in your namespace
	err = client.WatchBlobs(ctx, namespace, 9286352, 1*time.Second, func(height uint64, blobs []*grpcclient.Blob) error {
		fmt.Printf("Height %d: got %d blobs\n", height, len(blobs))

		for _, blob := range blobs {
			// Process blob data
			fmt.Printf("  - %d bytes\n", len(blob.Data))
		}

		return nil // return error to stop watching
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Fatal(err)
	}
}

