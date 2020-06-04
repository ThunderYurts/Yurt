package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThunderYurts/Yurt/action"
	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/ysync"
	"github.com/ThunderYurts/Yurt/yurt"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

const (
	logName = "./yurt.log"
)

func testAction(t *testing.T) {
	Convey("test put action twice", t, func() {
		rootContext := context.Background()
		conn, err := grpc.Dial(":50002", grpc.WithInsecure())
		So(err, ShouldBeNil)
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
	Convey("test primary and secondary", t, func() {
		primaryRootContext, primaryFinalizeFunc := context.WithCancel(context.Background())
		primaryLogName := "./primary.log"
		os.Truncate(primaryLogName, 0)
		primary, err := yurt.NewYurt(primaryRootContext, primaryFinalizeFunc, primaryLogName, false, yconst.PRIMARY)
		So(err, ShouldBeNil)
		err = primary.Start(":50001", ":50002")
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
		secondary, err := yurt.NewYurt(secondaryRootContext, secondaryFinalizeFunc, secondaryLogName, false, yconst.SECONDARY, ":50001")
		So(err, ShouldBeNil)
		err = secondary.Start(":50003", ":50004")
		So(err, ShouldBeNil)

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
}

func setup() (*yurt.Yurt, error) {
	rootContext, finalizeFunc := context.WithCancel(context.Background())
	os.Truncate(logName, 0)
	yurt, err := yurt.NewYurt(rootContext, finalizeFunc, logName, false, yconst.PRIMARY)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	err = yurt.Start(":50001", ":50002")
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return &yurt, nil
}

func teardown(yurt *yurt.Yurt) {
	yurt.Stop()
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
