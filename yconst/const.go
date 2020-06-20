package yconst

const (
	// PRIMARY for config
	PRIMARY = "primary"
	// SECONDARY for config secondary eg just read not put delete
	SECONDARY = "secondary"
	// ActionPut for algo param "put"
	ActionPut = "put"
	// ActionDelete for algo param "delete"
	ActionDelete = "delete"
	// ActionRead for algo param "read"
	ActionRead = "read"
	// YurtRoot is idle YurtRoot
	YurtRoot = "/yurt"
	// ServiceRoot is Service config root folder
	ServiceRoot = "/service"
	// EmptyIP will give a default value to register
	EmptyIP = ""
	// TotalSlotNum is the num of slots
	TotalSlotNum = 11384
	// ActionPort is Action Server start port
	ActionPort = ":8080"
	// SyncPort is Sync Server start port
	SyncPort = ":8000"
)
