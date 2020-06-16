package log

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Inline means a log is inline raw string without json or xml
type Inline struct {
	sync.Mutex
	filename string
	count    int32
	index    int32
	f        *os.File
}

// NewLogInline is an help function
func NewLogInline(filename string) (Inline, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil && os.IsNotExist(err) {
		f, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	}
	if err != nil {
		return Inline{}, err
	}

	return Inline{
		filename: filename,
		count:    0,
		index:    0,
		f:        f,
	}, nil
}

// NewLogInlineWithoutCreate is an help function
func NewLogInlineWithoutCreate(filename string) (Inline, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil && os.IsNotExist(err) {
		return Inline{}, errors.New(filename + " not found")
	}
	if err != nil {
		return Inline{}, err
	}

	return Inline{
		filename: filename,
		count:    0,
		index:    0,
		f:        f,
	}, nil
}

// Delete action write into log
func (log *Inline) Delete(key string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	commit := "D " + key + " " + strconv.Itoa(int(log.count)) + " \n"
	log.count = log.count + 1
	fmt.Println(commit)
	_, err := writer.WriteString(commit)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

// Put action write into log
func (log *Inline) Put(key string, value string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	commit := "P " + key + " " + value + " " + strconv.Itoa(int(log.count)) + " \n"
	fmt.Println(commit)
	log.count = log.count + 1
	_, err := writer.WriteString(commit)
	err = writer.Flush()
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

// ImportLog is read log from others and do it
func (log *Inline) ImportLog(logs []string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	for _, val := range logs {
		_, err := writer.WriteString(val + "\n")
		if err != nil {
			// TODO need rollback
			return err
		}
		fmt.Printf("sedcondary write %s\n", val)
	}
	err := writer.Flush()
	return err

}

// LoadLog from file
func (log *Inline) LoadLog(index int32) ([]string, int32, error) {
	if index != log.index {
		return nil, 0, errors.New("index not match")
	}
	reader := bufio.NewReader(log.f)
	lines := []string{}
	max := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// fmt.Println("File read buttom!")
				break
			} else {
				fmt.Println("Read file error!", err)
				return nil, 0, err
			}
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		lines = append(lines, line)
		max = max + 1
		if max == 10 {
			break
		}
	}
	if len(lines) > 0 {
		lastLogSplit := strings.Fields(lines[len(lines)-1])
		newIndex, err := strconv.ParseInt(lastLogSplit[len(lastLogSplit)-1], 10, 32)
		if err != nil {
			return nil, 0, err
		}
		log.index = int32(newIndex)
		return lines, int32(newIndex), nil
	}
	return lines, index, nil
}

// Destruct fd
func (log *Inline) Destruct() error {
	err := log.f.Close()
	return err
}
