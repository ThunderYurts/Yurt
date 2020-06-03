package log

// Log interface means write log into file
type Log interface {
	Delete(key string) error
	Put(key string, value string) error
	LoadLog(index int32) ([]string, int32, error)
	ImportLog(logs []string) error
	Destruct() error
}
