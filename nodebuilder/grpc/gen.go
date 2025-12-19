//go:build ignore
// +build ignore

package main

// Run this to generate protobuf code:
//   go generate ./nodebuilder/grpc/...
//
// Requires:
//   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pb/blob.proto

