package log

// Log interface means write log into file
type Log interface {
	Delete(key string, value string) error
	Put(key string, value string, oldValue string) error
	LoadLog(index int32) ([]string, int32, error)
	ImportLog(index int32, logs []string) error
	Destruct() error
	GetIndex() *int32
	SetIndex(index int32)
}
