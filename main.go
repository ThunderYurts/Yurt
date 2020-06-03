package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"sync"

	"github.com/ThunderYurts/Yurt/action"
	"github.com/ThunderYurts/Yurt/log"
	"github.com/ThunderYurts/Yurt/storage"
	"github.com/ThunderYurts/Yurt/ysync"
)

// Yurt is now here!
type Yurt struct {
	syncServer    ysync.Server
	actionServer  action.Server
	syncClient    ysync.LogSyncClient
	ctx           context.Context
	wg            sync.WaitGroup
	fianalizeFunc context.CancelFunc
	// config
}

// NewYurt is a help function to Init Yurt
func NewYurt(ctx context.Context, finalizeFunc context.CancelFunc, logName string, locked bool, lockecChan <-chan bool) (Yurt, error) {
	// TODO syncClient
	l, err := log.NewLogInline(logName)
	if err != nil {
		return Yurt{}, err
	}
	storage := storage.NewMemory()
	return Yurt{
		syncServer:    ysync.NewServer(ctx, logName),
		actionServer:  action.NewServer(ctx, &storage, &l, locked, lockecChan),
		syncClient:    nil,
		ctx:           ctx,
		wg:            sync.WaitGroup{},
		fianalizeFunc: finalizeFunc,
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
	err = yurt.actionServer.Start(actionPort, &yurt.wg)
	if err != nil {
		return err
	}

	return err
}

var (
	help       bool
	syncPort   string
	actionPort string
	logName    string
)

func init() {
	flag.BoolVar(&help, "h", false, "this help")
	flag.StringVar(&syncPort, "sp", ":50000", "set syncPort port")
	flag.StringVar(&actionPort, "ap", ":50001", "set actionPort port")
	flag.StringVar(&logName, "l", "./yurt.log", "set log name")
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}
	rootContext, finalizeFunc := context.WithCancel(context.Background())
	lockedChan := make(chan bool)
	yurt, err := NewYurt(rootContext, finalizeFunc, logName, false, lockedChan)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = yurt.Start(syncPort, actionPort)
	if err != nil {
		fmt.Println(err.Error())
	}
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt, os.Kill)

	// s := <-c

	// fmt.Println("Got signal:", s)
	time.Sleep(20 * time.Second)
	yurt.Stop()
}
