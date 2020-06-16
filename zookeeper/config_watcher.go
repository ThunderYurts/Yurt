package zookeeper

import (
	"context"
	"fmt"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

// ConfigWatcher is a zk watcher and use channel send latest data from zk
type ConfigWatcher struct {
	ctx     context.Context
	wg      *sync.WaitGroup
	channel chan<- []byte
	conn    *zk.Conn
	zname   string
}

// NewConfigWatcher will create conn with zkAddr
func NewConfigWatcher(ctx context.Context, wg *sync.WaitGroup, channel chan<- []byte, conn *zk.Conn, zname string) ConfigWatcher {
	return ConfigWatcher{
		ctx:     ctx,
		wg:      wg,
		channel: channel,
		conn:    conn,
		zname:   zname,
	}
}

// Start cw will watch data from zk
func (cw *ConfigWatcher) Start() {
	data, _, getCh, err := cw.conn.GetW(cw.zname)
	if err != nil {
		fmt.Printf("in config_watcher error :%v\n", err)
		return
	}
	if len(data) != 0 {
		cw.channel <- data
	}
	cw.wg.Add(1)
	go func() {
		defer cw.wg.Done()
		for {
			select {
			case e := <-getCh:
				{
					if e.Type == zk.EventNodeCreated {
						fmt.Printf("has new node[%s] create\n", e.Path)
					} else if e.Type == zk.EventNodeDeleted {
						fmt.Printf("has node[%s] delete\n", e.Path)
					} else if e.Type == zk.EventNodeDataChanged {
						data, _, getCh, err = cw.conn.GetW(e.Path)
						if err != nil {
							fmt.Printf(err.Error())
							panic(err)
						}
						cw.channel <- data
					}
				}
			case <-cw.ctx.Done():
				{
					fmt.Println("config watcher done")
					return
				}
			}
		}
	}()
}
