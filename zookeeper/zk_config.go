package zookeeper

//ZKServiceHost is a single key value pair for local ServiceHost
type ZKServiceHost struct {
	Service           string
	SyncHost          string
	Primary           string
	SecondarySyncHost string
	Secondary         []string
	SlotBegin         uint32
	SlotEnd           uint32
	Locked            bool
}

// ZKServiceRegister is used by yurt registed into a service and hold until die
type ZKServiceRegister struct {
	Host string
}

// ZKRegister is a register response to yurt temporary node
type ZKRegister struct {
	ServiceName string
}
