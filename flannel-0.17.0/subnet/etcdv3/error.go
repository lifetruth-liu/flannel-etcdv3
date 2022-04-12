package etcdv3

import (
	"google.golang.org/grpc/codes"
)

var (
	ErrKeyNotFound           = EtcdError{code: codes.NotFound, desc: "etcdserver: key is not found"}
	ErrKeyAlreadyExists      = EtcdError{code: codes.AlreadyExists, desc: "etcdserver: key is already exists"}
	ErrCodeEventIndexCleared = EtcdError{code: codes.OutOfRange, desc: "etcdserver: index to small"}
)

type EtcdError struct {
	code codes.Code
	desc string
}

func (e EtcdError) Code() codes.Code {
	return e.code
}

func (e EtcdError) Error() string {
	return e.desc
}
