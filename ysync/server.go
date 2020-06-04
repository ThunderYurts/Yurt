package ysync

import (
	"context"
	"fmt"
	"io"
	"net"
	sync "sync"

	"github.com/ThunderYurts/Yurt/log"
	grpc "google.golang.org/grpc"
)

// ServerConfig is config from yurt
type ServerConfig struct {
	Stage       string
	PrimaryAddr string
}

// Server for other sync
type Server struct {
	syncSeek map[string]log.Log
	ctx      context.Context
	logName  string
	config   *ServerConfig
}

// NewServer is a help function
func NewServer(ctx context.Context, logName string, config *ServerConfig) Server {
	return Server{
		syncSeek: make(map[string]log.Log),
		ctx:      ctx,
		logName:  logName,
		config:   config,
	}
}

// Sync which means we are giving logs to others
func (s *Server) Sync(stream LogSync_SyncServer) error {
	var name string
	for {
		select {
		case <-s.ctx.Done():
			{
				stream.Context().Done()
				return nil
			}
		default:
			{
				req, err := stream.Recv()
				if err == io.EOF {
					fmt.Println("close file")
					l, exist := s.syncSeek[name]
					if exist {
						err = l.Destruct()
						if err != nil {
							delete(s.syncSeek, name)
							return err
						}
					}
					delete(s.syncSeek, name)
					return nil
				}
				if err != nil {
					return err
				}
				index := req.Index
				name = req.Name
				if index < 0 {
					stream.Send(&SyncReply{Code: SyncCode_SYNC_ERROR})
					return nil
				}
				l, exist := s.syncSeek[req.Name]
				if !exist {
					n, err := log.NewLogInlineWithoutCreate(s.logName)
					if err != nil {
						return err
					}
					s.syncSeek[req.Name] = &n
					l = s.syncSeek[req.Name]
				}

				logs, index, err := l.LoadLog(index)
				// TODO can use buffer to reduce connection times
				if err != nil {
					// now maybe index not match
					return err
				}
				rep := &SyncReply{
					Code:      SyncCode_SYNC_SUCCESS,
					Logs:      logs,
					LastIndex: index,
				}
				err = stream.Send(rep)
				if err != nil {
					return err
				}
			}
		}
	}
}

// Start service for sync
func (s *Server) Start(port string, wg *sync.WaitGroup) error {
	syncServer := grpc.NewServer()
	RegisterLogSyncServer(syncServer, s)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		fmt.Printf("sync server listen on %s\n", port)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			select {
			case <-s.ctx.Done():
				{
					fmt.Println("sync get Done")
					syncServer.GracefulStop()
					return
				}
			}
		}(wg)
		syncServer.Serve(lis)
	}(wg)

	return nil
}
