package zookeeper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/samuel/go-zookeeper/zk"
)

// Preemptive is a common
type Preemptive struct {
	ctx            context.Context
	wg             *sync.WaitGroup
	conn           *zk.Conn
	preemptiveName string
	channel        chan string
}

// NewPreemptive is a help function to new Preemptive
func NewPreemptive(ctx context.Context, wg *sync.WaitGroup, conn *zk.Conn, PreemptiveName string, channel chan string) Preemptive {
	return Preemptive{
		ctx:            ctx,
		preemptiveName: PreemptiveName,
		wg:             wg,
		conn:           conn,
		channel:        channel,
	}

}

// Preemptive will try to preemptive a temporary node,
// if we do not get the node, we will retry in a forever loop
// if the primary is down, we will quickly hold the temporary node
// and then we will become the primary
func (p *Preemptive) Preemptive(data []byte) {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				{
					close(p.channel)
					return
				}
			default:
				{
					acl := zk.WorldACL(zk.PermAll)
					_, err := p.conn.Create(p.preemptiveName, data, zk.FlagEphemeral, acl)
					if err == nil {
						fmt.Printf("%s will hold service master\n", p.preemptiveName)
						p.channel <- yconst.PRIMARY
						return
					}
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()
}

// GetSessionID will return conn.SeSessionId
func (p *Preemptive) GetSessionID() int64 {
	return p.conn.SessionID()
}
