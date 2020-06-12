package zookeeper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/ThunderYurts/Yurt/yconst"
	"github.com/samuel/go-zookeeper/zk"
)

// Register will hold a temporary node which contains ZKRegister
type Register struct {
	host        string
	yurtName    string
	serviceName string
	conn        *zk.Conn
}

// NewRegister is a help function
func NewRegister(host string, yurtName string, conn *zk.Conn) Register {
	return Register{
		host:        host,
		yurtName:    yurtName,
		conn:        conn,
		serviceName: "",
	}
}

func (r *Register) Register() (string, error) {
	_, err := r.conn.Create(yconst.YurtRoot+"/"+r.yurtName, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println("34")
		fmt.Println(err.Error())
		return "", err
	}
	data, _, ch, err := r.conn.GetW(yconst.YurtRoot + "/" + r.yurtName)
	if err != nil {
		return "", err
	}

	if len(data) > 0 {
		// has get register response
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		reg := ZKRegister{}
		err = dec.Decode(&reg)
		r.serviceName = reg.ServiceName
		return reg.ServiceName, nil
	}
	for {
		select {
		case e := <-ch:
			{
				// zeus scheduler has work done
				if e.Type == zk.EventNodeDataChanged {
					data, _, _, err := r.conn.GetW(yconst.YurtRoot + "/" + r.yurtName)
					if err != nil {
						return "", err
					}
					if len(data) > 0 {
						// has get register response
						dec := gob.NewDecoder(bytes.NewBuffer(data))
						reg := ZKRegister{}
						err = dec.Decode(&reg)
						r.serviceName = reg.ServiceName
						return reg.ServiceName, nil
					}
					return "", errors.New("invalid register info format")
				}
			}
		}
	}
}

// ServiceRegister will use conn to create register node with ZKServiceRegister
func (r *Register) ServiceRegister(serviceName string, host string) error {
	regInfo := ZKServiceRegister{
		Host: host,
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(regInfo)
	if err != nil {
		return err
	}
	_, err = r.conn.Create(yconst.ServiceRoot+"/"+serviceName+"/yurt/"+r.yurtName, buf.Bytes(), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}
