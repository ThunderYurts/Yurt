package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/ThunderYurts/Yurt/yurt"
)

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
	
	yurt, err := yurt.NewYurt(rootContext, finalizeFunc, "primary", logName, false, yconst.PRIMARY)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = yurt.Start(syncPort, actionPort)
	if err != nil {
		fmt.Println(err.Error())
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	s := <-c

	fmt.Println("Got signal:", s)
	yurt.Stop()
}
