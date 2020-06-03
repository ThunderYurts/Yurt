package action

import (
	"context"
	"fmt"
	"net"
	sync "sync"

	"github.com/ThunderYurts/Yurt/log"
	"github.com/ThunderYurts/Yurt/storage"
	grpc "google.golang.org/grpc"
)

// Server for handling rpc request
type Server struct {
	storage    storage.Storage
	log        log.Log
	locked     bool
	lockedChan <-chan bool
	ctx        context.Context
}

// NewServer is a help function
func NewServer(ctx context.Context, storage storage.Storage, log log.Log, locked bool, lockedChan <-chan bool) Server {
	return Server{
		storage:    storage,
		log:        log,
		locked:     locked,
		lockedChan: lockedChan,
		ctx:        ctx,
	}
}

// Read from storage
func (s *Server) Read(ctx context.Context, in *ReadRequest) (*ReadReply, error) {
	value, err := s.storage.Read(in.Key)
	if err != nil {
		return nil, err
	}
	return &ReadReply{Value: value}, nil
}

// Put put key value pair into storage
func (s *Server) Put(ctx context.Context, in *PutRequest) (*PutReply, error) {
	// TODO check Lock first
	if s.locked {
		return &PutReply{Code: PutCode_PUT_LOCK}, nil
	}
	err := s.log.Put(in.Key, in.Value)
	if err != nil {
		return nil, err
	}

	err = s.storage.Put(in.Key, in.Value)

	if err != nil {
		return &PutReply{Code: PutCode_PUT_ERROR}, err
	}

	return &PutReply{Code: PutCode_PUT_SUCCESS}, nil

}

// Delete put key value pair into storage
func (s *Server) Delete(ctx context.Context, in *DeleteRequest) (*DeleteReply, error) {
	// TODO check Lock first
	if s.locked {
		return &DeleteReply{Code: DeleteCode_DELETE_LOCK}, nil
	}
	err := s.log.Delete(in.Key)
	if err != nil {
		return nil, err
	}

	err = s.storage.Delete(in.Key)

	if err != nil {
		return &DeleteReply{Code: DeleteCode_DELETE_ERROR}, err
	}

	return &DeleteReply{Code: DeleteCode_DELETE_SUCCESS}, nil

}

// Start service for action
func (s *Server) Start(port string, wg *sync.WaitGroup) error {
	actionServer := grpc.NewServer()
	RegisterActionServer(actionServer, s)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		fmt.Printf("action server listen on %s\n", port)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			select {
			case <-s.ctx.Done():
				{
					fmt.Println("action get Done")
					actionServer.GracefulStop()
					return
				}
			}
		}(wg)
		actionServer.Serve(lis)
	}(wg)
	return nil
}
