#!/bin/bash
set -eo pipefail
shopt -s nullglob

yurt_name="yurt"
if [ "$YURT_NAME" ] ;then
  yurt_name=$YURT_NAME
fi

action_port=":8080"
if [ "$ACTION_PORT" ] ;then
  action_port=$ACTION_PORT
fi

ip="127.0.0.1"
if [ "$HOST_IP" ] ;then
  ip=$HOST_IP
fi

log_path="./yurt.log"
if [ "$LOG_PATH" ] ;then
  log_path=$LOG_PATH
fi

sync_port=":8000"
if [ "$SYNC_PORT" ] ;then
  sync_port=$SYNC_PORT
fi

zookeeper_addr="106.15.225.249:3030"
if [ "$ZOOKEEPER_ADDR" ] ;then
  zookeeper_addr=$ZOOKEEPER_ADDR
fi

/root/app -n "$yurt_name" -ap "$action_port" -ip "$ip" -l "$log_path" -sp "$sync_port" -zk "$zookeeper_addr"