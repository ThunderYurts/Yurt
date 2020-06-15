package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/ThunderYurts/Yurt/yurt"
)

var (
	help       bool
	name       string
	syncPort   string
	actionPort string
	logName    string
	ip         string
	zkAddr     string
)

func init() {
	flag.BoolVar(&help, "h", false, "this help")
	flag.StringVar(&name, "n", "yurt", "set yurt name")
	flag.StringVar(&ip, "ip", "127.0.0.1", "set node ip")
	flag.StringVar(&syncPort, "sp", ":39999", "set syncPort port")
	flag.StringVar(&actionPort, "ap", ":40000", "set actionPort port")
	flag.StringVar(&logName, "l", "./yurt.log", "set log name")
	flag.StringVar(&zkAddr, "zk", "106.15.225.249:3030", "set zeus connection zookeeper cluster")
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}
	rootContext, finalizeFunc := context.WithCancel(context.Background())

	yurt, err := yurt.NewYurt(rootContext, finalizeFunc, name, logName)
	if err != nil {
		fmt.Println(err.Error())
	}
	addrs := strings.Split(zkAddr, ";")
	err = yurt.Start(ip, syncPort, actionPort, addrs)
	if err != nil {
		fmt.Println(err.Error())
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	s := <-c

	fmt.Println("Got signal:", s)
	yurt.Stop()
}
