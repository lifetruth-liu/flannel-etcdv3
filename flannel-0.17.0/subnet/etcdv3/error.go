package etcd3

import (
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	keyNotFound      = status.New(codes.NotFound, "etcdserver: key is not found").Err()
	keyAlreadyExists = status.New(codes.AlreadyExists, "etcdserver: key is already exists").Err()
)

var (
	ErrKeyNotFound      = rpctypes.Error(keyNotFound)
	ErrKeyAlreadyExists = rpctypes.Error(keyAlreadyExists)
)
