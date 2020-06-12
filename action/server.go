package action

import (
	"context"
	"fmt"
	"net"
	sync "sync"

	"github.com/ThunderYurts/Yurt/log"
	"github.com/ThunderYurts/Yurt/storage"
	"github.com/ThunderYurts/Yurt/yconst"
	grpc "google.golang.org/grpc"
)

// ServerConfig get config from yurt
type ServerConfig struct {
	Stage  string
	Locked bool
}

// Server for handling rpc request
type Server struct {
	storage storage.Storage
	log     log.Log
	ctx     context.Context
	config  *ServerConfig
	wg      *sync.WaitGroup
}

// NewServer is a help function
func NewServer(ctx context.Context, storage storage.Storage, log log.Log, config *ServerConfig, wg *sync.WaitGroup) Server {
	return Server{
		storage: storage,
		log:     log,
		ctx:     ctx,
		config:  config,
		wg:      wg,
	}
}

// Read from storage
func (s *Server) Read(ctx context.Context, in *ReadRequest) (*ReadReply, error) {
	value, err := s.storage.Read(in.Key)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			return &ReadReply{Code: ReadCode_READ_NOT_FOUND, Value: value}, nil
		}
		return nil, err
	}
	return &ReadReply{Code: ReadCode_READ_SUCCESS, Value: value}, nil
}

// Put put key value pair into storage
func (s *Server) Put(ctx context.Context, in *PutRequest) (*PutReply, error) {
	// TODO check Lock first
	if s.config.Stage == yconst.SECONDARY {
		return &PutReply{Code: PutCode_PUT_ERROR}, nil
	}
	if s.config.Locked {
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
	if s.config.Stage == yconst.SECONDARY {
		return &DeleteReply{Code: DeleteCode_DELETE_ERROR}, nil
	}
	if s.config.Locked {
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
func (s *Server) Start(port string) error {
	actionServer := grpc.NewServer()
	RegisterActionServer(actionServer, s)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	s.wg.Add(1)
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
		}(s.wg)
		actionServer.Serve(lis)
	}(s.wg)
	return nil
}
