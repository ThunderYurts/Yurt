package ysync

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/ThunderYurts/Yurt/storage"
	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
	"hash/crc32"
	"io"
	"net"
	"strconv"
	sync "sync"

	"github.com/ThunderYurts/Yurt/log"
	grpc "google.golang.org/grpc"
)

// ServerConfig is config from yurt
type ServerConfig struct {
	Stage    string
	SyncAddr string
}

// Server for other sync
type Server struct {
	syncSeek map[string]log.Log
	ctx      context.Context
	wg       *sync.WaitGroup
	logName  string
	config   *ServerConfig
	storage  storage.Storage
	log      log.Log
	conn     *zk.Conn
	serviceName string
}

// NewServer is a help function
func NewServer(ctx context.Context, logName string, config *ServerConfig, wg *sync.WaitGroup, storage storage.Storage, l log.Log) Server {
	return Server{
		syncSeek: make(map[string]log.Log),
		ctx:      ctx,
		logName:  logName,
		config:   config,
		wg:       wg,
		storage:  storage,
		log:      l,
	}
}

func (s *Server) SlotSync(ctx context.Context, in *SlotRequest) (reply *SlotReply, err error) {
	begin := in.Begin
	end := in.End
	keys := s.storage.KeySet()
	var logs []string
	count := 0
	for _, key := range keys {
		slot := crc32.ChecksumIEEE([]byte(key)) % yconst.TotalSlotNum
		fmt.Printf("slot: %v begin: %v end: %v\n", slot, begin, end)
		if slot >= begin && slot < end { // [begin, end)
			value, err := s.storage.Read(key)
			if err != nil {
				return nil, err
			}
			commit := "P " + key + " " + value + " " + "NOT_FOUND" + " " + strconv.Itoa(count)
			logs = append(logs, commit)
			count = count + 1
		}
	}
	// TODO change slots
	data, stat, err := s.conn.Get(yconst.ServiceRoot + "/"+ s.serviceName)
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	srv := zookeeper.ZKServiceHost{}
	err = dec.Decode(&srv)
	srv.SlotEnd = begin
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(srv)
	if err != nil {
		panic(err)
	}
	_, err = s.conn.Set(yconst.ServiceRoot + "/"+ s.serviceName, buf.Bytes(), stat.Version)
	if err != nil {
		panic(err)
	}
	fmt.Printf("logs: %v\n", logs)
	return &SlotReply{Code: SlotCode_SLOT_SUCCESS, Logs: logs}, nil

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
					err = stream.Send(&SyncReply{Code: SyncCode_SYNC_ERROR})
					return err
				}

				l, exist := s.syncSeek[req.Name]
				if !exist {
					n, err := log.NewLogInlineWithoutCreate(s.logName, s.log.GetIndex())
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
func (s *Server) Start(port string, conn *zk.Conn, serviceName string) error {
	s.serviceName = serviceName
	syncServer := grpc.NewServer()
	RegisterLogSyncServer(syncServer, s)
	s.conn = conn
	lis, err := net.Listen("tcp", yconst.SyncPort)
	if err != nil {
		return err
	}
	s.wg.Add(1)
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
		}(s.wg)
		syncServer.Serve(lis)
	}(s.wg)

	return nil
}
