package storage

// Storage is an interface about Yurt Storage (memory or disk)
type Storage interface {
	Delete(key string) error
	Put(key string, value string) error
	Read(key string) (string, error)
	LoadLog(logs []string) (int32, error)
}
