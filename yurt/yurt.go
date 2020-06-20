package yurt

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/ThunderYurts/Yurt/action"
	"github.com/ThunderYurts/Yurt/log"
	"github.com/ThunderYurts/Yurt/storage"
	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/ysync"
	"github.com/ThunderYurts/Yurt/zookeeper"
	mapset "github.com/deckarep/golang-set"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
)

// ConfigServer from zk and dispatch to other component
type ConfigServer struct {
	ActionServerConfig *action.ServerConfig
	SyncServerConfig   *ysync.ServerConfig
	LogName            string
	Name               string
	channel            chan []byte
}

// Update config from zk
func (cs *ConfigServer) Update(srv zookeeper.ZKServiceHost, stage string) {
	cs.ActionServerConfig.Locked = srv.Locked
	cs.ActionServerConfig.Stage = stage
	cs.SyncServerConfig.Stage = stage
	cs.SyncServerConfig.SyncAddr = srv.SecondarySyncHost
}

// Yurt is now here!
type Yurt struct {
	syncServer    ysync.Server
	actionServer  action.Server
	syncClient    ysync.LogSyncClient
	log           log.Log
	ctx           context.Context
	wg            *sync.WaitGroup
	fianalizeFunc context.CancelFunc
	storage       storage.Storage
	config        *ConfigServer
	configWatcher *zookeeper.ConfigWatcher
	register      *zookeeper.Register
	preemptive    *zookeeper.Preemptive
}

// NewYurt is a help function to Init Yurt
func NewYurt(ctx context.Context, finalizeFunc context.CancelFunc, name string, logName string) (Yurt, error) {
	l, err := log.NewLogInline(logName)
	if err != nil {
		return Yurt{}, err
	}
	storage := storage.NewMemory()
	actionServerConfig := &action.ServerConfig{}
	syncServerConfig := &ysync.ServerConfig{}
	channel := make(chan []byte, 100)
	wg := &sync.WaitGroup{}
	return Yurt{
		syncServer:    ysync.NewServer(ctx, logName, syncServerConfig, wg, &storage, &l),
		actionServer:  action.NewServer(ctx, &storage, &l, actionServerConfig, wg),
		syncClient:    nil,
		log:           &l,
		ctx:           ctx,
		wg:            wg,
		fianalizeFunc: finalizeFunc,
		storage:       &storage,
		config:        &ConfigServer{ActionServerConfig: actionServerConfig, SyncServerConfig: syncServerConfig, LogName: logName, Name: name, channel: channel},
		configWatcher: nil,
		preemptive:    nil,
		register:      nil,
	}, nil
}

// Stop by stop context
func (yurt *Yurt) Stop() {
	yurt.fianalizeFunc()
	fmt.Println("root context has cancel")
	yurt.wg.Wait()
}

// Start is yurt starts service function
func (yurt *Yurt) Start(ip string, syncPort string, actionPort string, zkAddr []string) error {
	err := yurt.syncServer.Start(syncPort)
	if err != nil {
		return err
	}
	fmt.Println("syncServer start")
	conn, _, err := zk.Connect(zkAddr, 5*time.Second)
	if err != nil {
		return err
	}
	reg := zookeeper.NewRegister(ip+actionPort, yurt.config.Name, conn)
	yurt.register = &reg
	service, err := yurt.register.Register()
	if err != nil {
		return err
	}
	fmt.Printf("zeus dispatch to service %s\n", service)
	err = yurt.register.ServiceRegister(service, ip+actionPort)
	if err != nil {
		fmt.Println("err in 107")
		return err
	}
	fmt.Println("yurt service register")
	preemptiveChan := make(chan string)
	p := zookeeper.NewPreemptive(yurt.ctx, yurt.wg, conn, yconst.ServiceRoot+"/"+service+"/"+yconst.PRIMARY, preemptiveChan)
	yurt.preemptive = &p
	yurt.preemptive.Preemptive([]byte{})
	fmt.Println("preemptive start")
	// update service secondary by watching yurt node
	yurt.wg.Add(1)
	go func() {
		defer yurt.wg.Done()
		for {
			select {
			case <-yurt.ctx.Done():
				{
					return
				}
			case _, ok := <-preemptiveChan:
				{
					if !ok {
						return
					}

					// stage in channel must be primary
					stableChildrenSet := mapset.NewSet()
					children, _, ch, err := conn.ChildrenW(yconst.ServiceRoot + "/" + service + "/yurt")
					if err != nil {
						return
					}
					if len(children) != 0 {
						childrenSet := mapset.NewSet()
						for _, child := range children {
							childrenSet.Add(child)
						}
						if childrenSet.Difference(stableChildrenSet).Cardinality() == 0 {
							// no diff
							continue
						}
						stableChildrenSet = childrenSet
						newSecondary := []string{}
						for _, child := range stableChildrenSet.ToSlice() {
							data, _, err := conn.Get(yconst.ServiceRoot + "/" + service + "/yurt/" + child.(string))
							if err != nil {
								// maybe died at time we iterator
								continue
							}
							dec := gob.NewDecoder(bytes.NewBuffer(data))
							second := zookeeper.ZKServiceRegister{}
							err = dec.Decode(&second)
							if err != nil {
								panic(err)
							}
							if second.Host != ip+actionPort {
								newSecondary = append(newSecondary, second.Host)
							}
						}
						data, stat, err := conn.Get(yconst.ServiceRoot + "/" + service)
						if err != nil {
							panic(err)
						}
						dec := gob.NewDecoder(bytes.NewBuffer(data))
						srv := zookeeper.ZKServiceHost{}
						err = dec.Decode(&srv)
						if err != nil {
							panic(err)
						}
						// TODO syncHost check and sync Data
						fmt.Printf("original srv value %v\n", srv)
						if srv.SyncHost != "" {
							// we need sync data from others
							// get now primary from serviceHost
							data, _, err := conn.Get(yconst.ServiceRoot + "/" + srv.SyncHost)
							dec := gob.NewDecoder(bytes.NewBuffer(data))
							syncTarget := zookeeper.ZKServiceHost{}
							err = dec.Decode(&syncTarget)
							if err != nil {
								panic(err)
							}
							fmt.Printf("syncTarget %v\n", syncTarget.SecondarySyncHost)
							connAction, err := grpc.Dial(syncTarget.SecondarySyncHost, grpc.WithInsecure())
							if err != nil {
								panic(err)
							}
							yurt.syncClient = ysync.NewLogSyncClient(connAction)
							slotReply, err := yurt.syncClient.SlotSync(yurt.ctx, &ysync.SlotRequest{Begin: srv.SlotBegin, End: srv.SlotEnd})
							if err != nil {
								panic(err)
							}
							if slotReply.Code != ysync.SlotCode_SLOT_SUCCESS {
								panic(slotReply)
							}
							fmt.Println(slotReply.Logs)
							err = yurt.storage.LoadLog(slotReply.Logs)
							if err != nil {
								panic(err)
							}
							srv.SyncHost = ""
						}
						srv.Secondary = newSecondary
						srv.Primary = ip + actionPort
						srv.SecondarySyncHost = ip + syncPort
						fmt.Printf("will update new srv %v\n", srv)
						buf := new(bytes.Buffer)
						enc := gob.NewEncoder(buf)
						err = enc.Encode(srv)
						if err != nil {
							panic(err)
						}
						_, err = conn.Set(yconst.ServiceRoot+"/"+service, buf.Bytes(), stat.Version)
						if err != nil {
							panic(err)
						}
					}
					for {
						select {
						case e := <-ch:
							{
								if e.Type == zk.EventNodeChildrenChanged {
									children, _, ch, err = conn.ChildrenW(yconst.ServiceRoot + "/" + service + "/yurt")
									childrenSet := mapset.NewSet()
									fmt.Printf("children %v\n", children)
									for _, child := range children {
										childrenSet.Add(child)
									}
									if childrenSet.Difference(stableChildrenSet).Cardinality() == 0 && stableChildrenSet.Difference(childrenSet).Cardinality() == 0 {
										// no diff
										fmt.Println("no diff")
										continue
									}
									stableChildrenSet = childrenSet
									newSecondary := []string{}
									for _, child := range stableChildrenSet.ToSlice() {
										data, _, err := conn.Get(yconst.ServiceRoot + "/" + service + "/yurt/" + child.(string))
										if err != nil {
											// maybe died at time we iterator
											continue
										}
										dec := gob.NewDecoder(bytes.NewBuffer(data))
										second := zookeeper.ZKServiceRegister{}
										err = dec.Decode(&second)
										if err != nil {
											panic(err)
										}
										if second.Host != ip+actionPort {
											newSecondary = append(newSecondary, second.Host)
										}
									}
									data, stat, err := conn.Get(yconst.ServiceRoot + "/" + service)
									if err != nil {
										panic(err)
									}
									dec := gob.NewDecoder(bytes.NewBuffer(data))
									srv := zookeeper.ZKServiceHost{}
									err = dec.Decode(&srv)
									if err != nil {
										panic(err)
									}
									srv.Secondary = newSecondary
									srv.Primary = ip + actionPort
									srv.SecondarySyncHost = ip + syncPort
									fmt.Printf("will update new srv %v\n", srv)
									buf := new(bytes.Buffer)
									enc := gob.NewEncoder(buf)
									err = enc.Encode(srv)
									if err != nil {
										panic(err)
									}
									_, err = conn.Set(yconst.ServiceRoot+"/"+service, buf.Bytes(), stat.Version)
									if err != nil {
										panic(err)
									}
								}
							}
						case <-yurt.ctx.Done():
							{
								return
							}
						}
					}
				}
			}
		}
	}()

	cw := zookeeper.NewConfigWatcher(yurt.ctx, yurt.wg, yurt.config.channel, conn, yconst.ServiceRoot+"/"+service)
	yurt.configWatcher = &cw
	yurt.configWatcher.Start()

	// TODO start sync from zk
	syncChannel := make(chan bool)

	go func() {
		for {
			select {
			case data := <-yurt.config.channel:
				{
					dec := gob.NewDecoder(bytes.NewBuffer(data))
					srv := zookeeper.ZKServiceHost{}
					err = dec.Decode(&srv)
					if err != nil {
						panic(err)
					}
					fmt.Printf("get config %v\n", srv)
					if srv.Primary == "" {
						continue
					}
					if srv.Primary != ip+actionPort {
						// I'm not primary start sync
						fmt.Printf("I'm not primary start sync %s\n", ip+syncPort)
						if srv.SecondarySyncHost == yurt.config.SyncServerConfig.SyncAddr {
							// just maybe a secondary changed or locked changed
							yurt.config.Update(srv, yconst.SECONDARY)
							continue
						} else {
							// Primary changed but not me
							fmt.Println("326 Primary changed but not me")
							yurt.config.Update(srv, yconst.SECONDARY)
							syncChannel <- true
						}
					} else {
						// I'm primary now
						fmt.Println("I'm primary now")
						yurt.config.Update(srv, yconst.PRIMARY)
						syncChannel <- false
					}
				}
			case <-yurt.ctx.Done():
				{
					conn.Close()
					return
				}
			}
		}
	}()

	yurt.wg.Add(1)
	go func() {
		defer yurt.wg.Done()
		fmt.Println("secondary starts sync routine")
		var syncLog log.Log
		syncLog = yurt.log
	sync:
		for {
			select {
			case <-yurt.ctx.Done():
				{
					return
				}
			default:
				{
					if yurt.config.SyncServerConfig.SyncAddr != ip + syncPort && yurt.config.SyncServerConfig.SyncAddr != "" {
						fmt.Printf("yurt.config.SyncServerConfig.SyncAddr = %s\n", yurt.config.SyncServerConfig.SyncAddr)
						break sync
					}
				}
			}
			time.Sleep(1 * time.Second)
		}
		syncConn, err := grpc.Dial(yurt.config.SyncServerConfig.SyncAddr, grpc.WithInsecure())
		if err != nil {
			fmt.Println("336")
			fmt.Println(err.Error())
			return
		}
		// create stream
		syncClient := ysync.NewLogSyncClient(syncConn)
		stream, err := syncClient.Sync(yurt.ctx)
		if err != nil {
			fmt.Println("344")
			fmt.Println(err.Error())
			return
		}
		// TODO index should load from dfile
		index := syncLog.GetIndex()
		fmt.Printf("current index %v\n", *index)
		for {
			select {
			case <-yurt.ctx.Done():
				{
					fmt.Println("sync routine done")
					stream.CloseSend()
					return
				}
			case synced := <-syncChannel:
				{
					if synced {
						// primary changed
						syncConn, err = grpc.Dial(yurt.config.SyncServerConfig.SyncAddr, grpc.WithInsecure())
						if err != nil {
							fmt.Println("362")
							fmt.Println(err.Error())
							return
						}
						fmt.Printf("create new syncConn from %v\n", yurt.config.SyncServerConfig.SyncAddr)
						// create stream
						syncClient = ysync.NewLogSyncClient(syncConn)
						stream, err = syncClient.Sync(yurt.ctx)
						if err != nil {
							fmt.Println("370")
							fmt.Println(err.Error())
							return
						}
					} else {
						// just return we are master
						return
					}
				}
			default:
				{
					// TODO use yurt name
					err = stream.Send(&ysync.SyncRequest{Name: yurt.config.Name, Index: *index})
					//fmt.Printf("send index %v\n", index)
					res, err := stream.Recv()
					if err != nil {
						//fmt.Println("385")
						//fmt.Println(err.Error())
						continue
					}
					if res.Code == ysync.SyncCode_SYNC_ERROR {
						fmt.Println("427")
						return
					}
					if res.LastIndex < *index {
						fmt.Printf("res.LastIndex: %v, index:%v\n", res.LastIndex, *index)
					}
					// TODO can do this parallel with lower
					logs := res.Logs
					if len(logs) != 0 {
						fmt.Printf("log log :%v\n", logs)
					}
					err = syncLog.ImportLog(res.LastIndex, logs)
					if err != nil {
						fmt.Println("396")
						fmt.Println(err.Error())
						return
					}
					// we will check whether logs should redo
					*index = res.LastIndex
					err = yurt.storage.LoadLog(logs)
					if err != nil {
						fmt.Println("396")
						fmt.Println(err.Error())
						return
					}
				}
			}
		}
	}()

	err = yurt.actionServer.Start(actionPort)
	if err != nil {
		return err
	}

	return err
}
