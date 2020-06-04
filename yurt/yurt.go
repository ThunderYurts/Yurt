package yurt

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThunderYurts/Yurt/action"
	"github.com/ThunderYurts/Yurt/log"
	"github.com/ThunderYurts/Yurt/storage"
	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/ysync"
	"google.golang.org/grpc"
)

// Config from zk
type Config struct {
	actionServerConfig *action.ServerConfig
	syncServerConfig   *ysync.ServerConfig
	logName            string
	name               string
}

// Yurt is now here!
type Yurt struct {
	syncServer    ysync.Server
	actionServer  action.Server
	syncClient    ysync.LogSyncClient
	ctx           context.Context
	wg            sync.WaitGroup
	fianalizeFunc context.CancelFunc
	storage       storage.Storage
	config        Config
}

// NewYurt is a help function to Init Yurt
func NewYurt(ctx context.Context, finalizeFunc context.CancelFunc, name string, logName string, locked bool, stage string, ops ...string) (Yurt, error) {
	l, err := log.NewLogInline(logName)
	if err != nil {
		return Yurt{}, err
	}
	storage := storage.NewMemory()
	actionServerConfig := &action.ServerConfig{Stage: stage, Locked: locked}
	addr := ""
	if stage == yconst.SECONDARY && len(ops) > 0 {
		addr = ops[0]
	}
	syncServerConfig := &ysync.ServerConfig{Stage: stage, PrimaryAddr: addr}
	return Yurt{
		syncServer:    ysync.NewServer(ctx, logName, syncServerConfig),
		actionServer:  action.NewServer(ctx, &storage, &l, actionServerConfig),
		syncClient:    nil,
		ctx:           ctx,
		wg:            sync.WaitGroup{},
		fianalizeFunc: finalizeFunc,
		storage:       &storage,
		config:        Config{actionServerConfig: actionServerConfig, syncServerConfig: syncServerConfig, logName: logName, name: name},
	}, nil
}

// Stop by stop context
func (yurt *Yurt) Stop() {
	yurt.fianalizeFunc()
	fmt.Println("root context has cancel")
	yurt.wg.Wait()
}

// Start is yurt starts service function
func (yurt *Yurt) Start(syncPort string, actionPort string) error {
	err := yurt.syncServer.Start(syncPort, &yurt.wg)
	if err != nil {
		return err
	}

	if yurt.config.syncServerConfig.Stage == yconst.SECONDARY {
		yurt.wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			fmt.Println("secondary starts sync routine")
			l, err := log.NewLogInline(yurt.config.logName)
			if err != nil {
				return
			}
			var syncLog log.Log
			syncLog = &l
			syncConn, err := grpc.Dial(yurt.config.syncServerConfig.PrimaryAddr, grpc.WithInsecure())
			// create stream
			syncClient := ysync.NewLogSyncClient(syncConn)
			stream, err := syncClient.Sync(yurt.ctx)
			index := int32(0)
			for {
				select {
				case <-yurt.ctx.Done():
					{
						fmt.Println("sync routine done")
						stream.CloseSend()
						return
					}
				default:
					{
						// TODO use yurt name
						err = stream.Send(&ysync.SyncRequest{Name: yurt.config.name, Index: index})
						res, err := stream.Recv()
						if err != nil {
							fmt.Println(err.Error())
							return
						}
						if res.Code == ysync.SyncCode_SYNC_ERROR {
							return
						}
						// TODO can do this parallel with lower
						logs := res.Logs
						err = syncLog.ImportLog(logs)
						if err != nil {
							fmt.Println(err.Error())
							return
						}
						index = res.LastIndex
						yurt.storage.LoadLog(logs)
					}
				}
			}
		}(&yurt.wg)
	}

	err = yurt.actionServer.Start(actionPort, &yurt.wg)
	if err != nil {
		return err
	}

	return err
}
