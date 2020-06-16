package storage

import (
	"errors"
	"strconv"
	"strings"
	"sync"
)

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
	return "", errors.New("NOT_FOUND")
}

// LoadLog will LoadLog from string and do it to sync data from Primary
func (m *Memory) LoadLog(logs []string) error {
	m.Lock()
	defer m.Unlock()
	/*  The log is format like this
	Delete : D {key} {index}
	Put: P {key} {value} {index}
	*/
	// check whether logs is right
	for _, log := range logs {
		action := log[0]
		switch action {
		case 'D':
			{
				// Delete action
				split := strings.Fields(log)
				if len(split) != 3 {
					return errors.New("log invalid delete")
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
				if len(split) != 4 {
					return errors.New("log invalid put")
				}
				_, err := strconv.ParseInt(split[len(split)-1], 10, 32)
				if err != nil {
					return err
				}
			}
		default:
			{
				return errors.New("log invalid action")
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
