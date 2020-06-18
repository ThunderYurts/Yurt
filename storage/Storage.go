package storage

import (
	"errors"
	"strconv"
	"strings"
	"sync"
)

var INVALID_DELETE = errors.New("log invalid delete")

var INVALID_PUT = errors.New("log invalid put")

var INVALID_ACTION = errors.New("log invalid action")

var NOT_FOUND = errors.New("NOT_FOUND")
// Memory will implement Storage in memory
type Memory struct {
	sync.Mutex
	mapping map[string]string
}

// NewMemory is a help function
func NewMemory() Memory {
	return Memory{
		mapping: make(map[string]string),
	}
}

// Delete a key in map
func (m *Memory) Delete(key string) error {
	m.Lock()
	delete(m.mapping, key)
	m.Unlock()
	return nil
}

// Put a key in map (overwriten)
func (m *Memory) Put(key string, value string) error {
	m.Lock()
	m.mapping[key] = value
	m.Unlock()
	return nil
}

func (m *Memory) Read(key string) (string, error) {
	m.Lock()
	value, exist := m.mapping[key]
	if exist {
		m.Unlock()
		return value, nil
	}
	m.Unlock()
	return "", NOT_FOUND
}

// LoadLog will LoadLog from string and do it to sync data from Primary
func (m *Memory) LoadLog(logs []string) error {
	m.Lock()
	defer m.Unlock()
	/*  The log is format like this
	Delete : D {key} {value} {index}
	Put: P {key} {value} {oldValue} {index}
	*/
	// check whether logs is right
	for _, log := range logs {
		action := log[0]
		switch action {
		case 'D':
			{
				// Delete action
				split := strings.Fields(log)
				if len(split) != 4 {
					return INVALID_DELETE
				}
				_, err := strconv.ParseInt(split[len(split)-1], 10, 32)
				if err != nil {
					return err
				}

			}
		case 'P':
			{
				// Put Action
				split := strings.Fields(log)
				if len(split) != 5 {
					return INVALID_PUT
				}
				_, err := strconv.ParseInt(split[len(split)-1], 10, 32)
				if err != nil {
					return err
				}
			}
		default:
			{
				return INVALID_ACTION
			}
		}
	}
	// redo logs
	for _, log := range logs {
		action := log[0]
		switch action {
		case 'D':
			{
				// Delete action
				split := strings.Fields(log)
				delete(m.mapping, split[1])
			}
		case 'P':
			{
				// Put Action
				split := strings.Fields(log)
				m.mapping[split[1]] = split[2]
			}
		}
	}

	return nil
}

func (m *Memory) KeySet() []string {
	// TODO check whether the lock is needed
	m.Lock()
	j := 0
	keys := make([]string, len(m.mapping))
	for k := range m.mapping {
		keys[j] = k
		j++
	}
	m.Unlock()
	return keys
}
