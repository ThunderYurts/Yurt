package test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThunderYurts/Yurt/action"
	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/ysync"
	"github.com/ThunderYurts/Yurt/yurt"
	"github.com/ThunderYurts/Yurt/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

const (
	logName = "./yurt.log"
)

func testAction(t *testing.T) {
	Convey("test put action twice", t, func() {
		time.Sleep(2 * time.Second)
		// fake scheduler
		zkConn, _, err := zk.Connect([]string{"localhost:2181"}, 5*time.Second)
		So(err, ShouldBeNil)
		data, stat, err := zkConn.Get(yconst.YurtRoot + "/primary")
		So(data, ShouldHaveLength, 0)
		reg0 := zookeeper.ZKRegister{ServiceName: "srv1"}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(reg0)
		So(err, ShouldBeNil)
		_, err = zkConn.Set(yconst.YurtRoot+"/primary", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)
		conn, err := grpc.Dial(":50002", grpc.WithInsecure())
		rootContext := context.Background()
		So(err, ShouldBeNil)
		time.Sleep(2 * time.Second)
		// create stream
		client := action.NewActionClient(conn)
		putRes, err := client.Put(rootContext, &action.PutRequest{Key: "animal", Value: "pig"})
		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_SUCCESS)

		readRes, err := client.Read(rootContext, &action.ReadRequest{Key: "animal"})

		So(err, ShouldBeNil)

		So(readRes.Code, ShouldEqual, action.ReadCode_READ_SUCCESS)

		So(readRes.Value, ShouldEqual, "pig")

		readRes, err = client.Read(rootContext, &action.ReadRequest{Key: "person"})

		So(err, ShouldBeNil)

		So(readRes.Code, ShouldEqual, action.ReadCode_READ_NOT_FOUND)

		putRes, err = client.Put(rootContext, &action.PutRequest{Key: "animal", Value: "taka"})

		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_SUCCESS)

		readRes, err = client.Read(rootContext, &action.ReadRequest{Key: "animal"})

		So(err, ShouldBeNil)

		So(readRes.Code, ShouldEqual, action.ReadCode_READ_SUCCESS)

		So(readRes.Value, ShouldEqual, "taka")
	})

	Convey("test delete", t, func() {
		rootContext := context.Background()
		conn, err := grpc.Dial(":50002", grpc.WithInsecure())
		So(err, ShouldBeNil)
		// create stream
		client := action.NewActionClient(conn)

		deleteRes, err := client.Delete(rootContext, &action.DeleteRequest{Key: "animal"})
		So(err, ShouldBeNil)
		So(deleteRes.Code, ShouldEqual, action.DeleteCode_DELETE_SUCCESS)
		putRes, err := client.Put(rootContext, &action.PutRequest{Key: "animal", Value: "pig"})
		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_SUCCESS)

		readRes, err := client.Read(rootContext, &action.ReadRequest{Key: "animal"})

		So(err, ShouldBeNil)

		So(readRes.Code, ShouldEqual, action.ReadCode_READ_SUCCESS)

		So(readRes.Value, ShouldEqual, "pig")

		deleteRes, err = client.Delete(rootContext, &action.DeleteRequest{Key: "animal"})
		So(err, ShouldBeNil)
		So(deleteRes.Code, ShouldEqual, action.DeleteCode_DELETE_SUCCESS)

		readRes, err = client.Read(rootContext, &action.ReadRequest{Key: "animal"})

		So(err, ShouldBeNil)

		So(readRes.Code, ShouldEqual, action.ReadCode_READ_NOT_FOUND)
	})

	Convey("test load log", t, func() {
		rootContext := context.Background()
		syncConn, err := grpc.Dial(":50001", grpc.WithInsecure())
		So(err, ShouldBeNil)
		// create stream
		syncClient := ysync.NewLogSyncClient(syncConn)
		stream, err := syncClient.Sync(rootContext)
		So(err, ShouldBeNil)
		err = stream.Send(&ysync.SyncRequest{Name: "slave", Index: 0})
		So(err, ShouldBeNil)
		res, err := stream.Recv()
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, ysync.SyncCode_SYNC_SUCCESS)
		So(res.Logs, ShouldHaveLength, 5)
		So(res.LastIndex, ShouldEqual, 4)

		// start a new client to make more

		actionConn, err := grpc.Dial(":50002", grpc.WithInsecure())
		So(err, ShouldBeNil)
		// create stream
		actionClient := action.NewActionClient(actionConn)

		putRes, err := actionClient.Put(rootContext, &action.PutRequest{Key: "fy", Value: "god"})
		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_SUCCESS)

		err = stream.Send(&ysync.SyncRequest{Name: "slave", Index: res.LastIndex})
		So(err, ShouldBeNil)
		res, err = stream.Recv()
		So(err, ShouldBeNil)
		So(res.Code, ShouldEqual, ysync.SyncCode_SYNC_SUCCESS)
		So(res.Logs, ShouldHaveLength, 1)
		So(res.LastIndex, ShouldEqual, 5)
		err = stream.CloseSend()
		So(err, ShouldBeNil)
	})
}

func testPrimarySecondaryInit(t *testing.T) {
	zkAddr := []string{"localhost:2181"}
	ip := "127.0.0.1"
	conn, _, _ := zk.Connect(zkAddr, 5*time.Second)
	for _, path := range []string{yconst.YurtRoot, yconst.ServiceRoot} {
		deleteNode(path, conn)
	}
	init := zookeeper.ZKServiceHost{SlotBegin: 0, SlotEnd: 11384}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	_ = enc.Encode(init)
	_, _ = conn.Create(yconst.ServiceRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
	_, _ = conn.Create(yconst.YurtRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
	_, _ = conn.Create(yconst.ServiceRoot+"/srv1", buf.Bytes(), 0, zk.WorldACL(zk.PermAll))
	_, _ = conn.Create(yconst.ServiceRoot+"/srv1/yurt", buf.Bytes(), 0, zk.WorldACL(zk.PermAll))

	Convey("test primary and secondary", t, func() {
		primaryRootContext, primaryFinalizeFunc := context.WithCancel(context.Background())
		zkAddr := []string{"localhost:2181"}
		primaryLogName := "./primary.log"
		os.Truncate(primaryLogName, 0)
		primary, err := yurt.NewYurt(primaryRootContext, primaryFinalizeFunc, "primary", primaryLogName)
		So(err, ShouldBeNil)
		go func() {
			err = primary.Start(ip, ":50001", ":50002", zkAddr)
			if err != nil {
				fmt.Println("180")
				fmt.Println(err.Error())
			}
		}()
		time.Sleep(1 * time.Second)
		data, stat, err := conn.Get(yconst.YurtRoot + "/primary")
		So(err, ShouldBeNil)
		So(data, ShouldHaveLength, 0)
		reg := zookeeper.ZKRegister{ServiceName: "srv1"}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(reg)
		So(err, ShouldBeNil)
		_, err = conn.Set(yconst.YurtRoot+"/primary", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)
		actionConn, err := grpc.Dial(":50002", grpc.WithInsecure())
		So(err, ShouldBeNil)
		// create stream
		actionClient := action.NewActionClient(actionConn)

		putRes, err := actionClient.Put(primaryRootContext, &action.PutRequest{Key: "fy", Value: "god"})
		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_SUCCESS)

		secondaryRootContext, secondaryFinalizeFunc := context.WithCancel(context.Background())
		secondaryLogName := "./secondary.log"
		os.Truncate(secondaryLogName, 0)
		secondary, err := yurt.NewYurt(secondaryRootContext, secondaryFinalizeFunc, "secondary", secondaryLogName)
		So(err, ShouldBeNil)
		go func() {
			err = secondary.Start(ip, ":50003", ":50004", zkAddr)
			if err != nil {
				fmt.Println("213")
				fmt.Println(err.Error())
			}
		}()
		time.Sleep(1 * time.Second)
		data, stat, err = conn.Get(yconst.YurtRoot + "/secondary")
		So(err, ShouldBeNil)
		So(data, ShouldHaveLength, 0)
		reg = zookeeper.ZKRegister{ServiceName: "srv1"}
		buf = new(bytes.Buffer)
		enc = gob.NewEncoder(buf)
		err = enc.Encode(reg)
		So(err, ShouldBeNil)
		_, err = conn.Set(yconst.YurtRoot+"/secondary", buf.Bytes(), stat.Version)
		So(err, ShouldBeNil)
		time.Sleep(1 * time.Second)
		actionConn, err = grpc.Dial(":50004", grpc.WithInsecure())
		So(err, ShouldBeNil)
		// create stream
		actionClient = action.NewActionClient(actionConn)

		putRes, err = actionClient.Put(primaryRootContext, &action.PutRequest{Key: "fy", Value: "god"})
		So(err, ShouldBeNil)

		So(putRes.Code, ShouldEqual, action.PutCode_PUT_ERROR)

		deleteRes, err := actionClient.Delete(primaryRootContext, &action.DeleteRequest{Key: "fy"})
		So(err, ShouldBeNil)

		So(deleteRes.Code, ShouldEqual, action.DeleteCode_DELETE_ERROR)
		time.Sleep(1 * time.Second)
		readRes, err := actionClient.Read(primaryRootContext, &action.ReadRequest{Key: "fy"})
		So(err, ShouldBeNil)
		So(readRes.Code, ShouldEqual, action.ReadCode_READ_SUCCESS)
		So(readRes.Value, ShouldEqual, "god")

		primary.Stop()
		secondary.Stop()
	})

	deleteNode(yconst.ServiceRoot, conn)
	deleteNode(yconst.YurtRoot, conn)
	deleteNode(yconst.ServiceRoot, conn)
}

func setup() (*yurt.Yurt, error) {
	zkAddr := []string{"localhost:2181"}
	ip := "127.0.0.1"
	conn, _, err := zk.Connect(zkAddr, 5*time.Second)
	for _, path := range []string{yconst.YurtRoot, yconst.ServiceRoot} {
		deleteNode(path, conn)
	}
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	init := zookeeper.ZKServiceHost{SlotBegin: 0, SlotEnd: 11384}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(init)
	_, err = conn.Create(yconst.ServiceRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
	_, err = conn.Create(yconst.YurtRoot, []byte{}, 0, zk.WorldACL(zk.PermAll))
	_, err = conn.Create(yconst.ServiceRoot+"/srv1", buf.Bytes(), 0, zk.WorldACL(zk.PermAll))

	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	conn.Create(yconst.ServiceRoot+"/srv1/yurt", []byte{}, 0, zk.WorldACL(zk.PermAll))
	rootContext, finalizeFunc := context.WithCancel(context.Background())
	os.Truncate(logName, 0)
	yurt, err := yurt.NewYurt(rootContext, finalizeFunc, "primary", logName)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	go func() {
		err = yurt.Start(ip, ":50001", ":50002", zkAddr)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}()

	// mock add service and register
	return &yurt, nil
}

func deleteNode(path string, conn *zk.Conn) {
	exist, stat, _ := conn.Exists(path)
	if exist {
		// find child first
		children, _, _ := conn.Children(path)
		for _, child := range children {
			deleteNode(path+"/"+child, conn)
		}
		_ = conn.Delete(path, stat.Version)
	}
}

func teardown(yurt *yurt.Yurt) {
	yurt.Stop()
	zkAddr := []string{"localhost:2181"}
	conn, _, err := zk.Connect(zkAddr, 5*time.Second)
	if err != nil {
		panic(err)
	}
	deleteNode(yconst.ServiceRoot, conn)
}

func TestEverything(t *testing.T) {
	// Maintaining this map is error-prone and cumbersome (note the subtle bug):
	fs := map[string]func(*testing.T){
		"testAction":               testAction,
		"testPrimarySecondaryInit": testPrimarySecondaryInit,
	}
	// You may be able to use the `importer` package to enumerate tests instead,
	// but that starts getting complicated.
	for name, f := range fs {
		if name != "testPrimarySecondaryInit" {
			yurt, err := setup()
			if err != nil {
				t.Error(err.Error())
			}
			t.Run(name, f)
			teardown(yurt)
		} else {
			t.Run(name, f)
		}
	}
}
