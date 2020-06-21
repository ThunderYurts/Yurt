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
	index    *int32
	f        *os.File
}

// NewLogInline is an help function
func NewLogInline(filename string) (Inline, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil && os.IsNotExist(err) {
		fmt.Println("new create")
		f, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return Inline{}, err
		}
		index := new(int32)
		*index = 0
		return Inline{
			filename: filename,
			count:    0,
			index:    index,
			f:        f,
		}, nil
	}
	if err != nil {
		fmt.Println("39")
		return Inline{}, err
	}
	cursor := int64(-1)
	for {
		buf := make([]byte, 1)
		_, _ = f.Seek(cursor, io.SeekEnd)
		_, err := f.Read(buf)
		//fmt.Printf("cusor %v read buf %v\n", cursor, buf)
		if err != nil {
			if err == io.EOF {
				index := new(int32)
				*index = 0
				return Inline{
					filename: filename,
					count:    0,
					index:    index,
					f:        f,
				}, nil
			}
			return Inline{}, err
		}
		if buf[0] == 32 {  // is a blank
			cursor = cursor + 1
			break
		}
		cursor = cursor - 1;
	}
	
	_, _ = f.Seek(cursor, io.SeekEnd)
	buf := make([]byte, -cursor - 1)
	_, err = f.Read(buf)
	if err != nil {
		fmt.Println("61")
		return Inline{}, err
	}
	freshIndex, err := strconv.Atoi(string(buf))
	fmt.Printf("get index %v\n", freshIndex)
	if err != nil {
		return Inline{}, err
	}
	index := new(int32)
	*index = int32(freshIndex)
	f.Seek(0, io.SeekEnd)
	return Inline{
		filename: filename,
		count:    *index + 1,
		index:    index,
		f:        f,
	}, nil
}

// NewLogInlineWithoutCreate is an help function
func NewLogInlineWithoutCreate(filename string, index *int32) (Inline, error) {
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
		index:    index,
		f:        f,
	}, nil
}

// Delete action write into log
func (log *Inline) Delete(key string, value string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	commit := "D " + key + " " + value + " " + strconv.Itoa(int(log.count)) + "\n"
	log.count = log.count + 1
	*log.index += 1
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
func (log *Inline) Put(key string, value string, oldValue string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	commit := "P " + key + " " + value + " " + oldValue + " " + strconv.Itoa(int(log.count)) + "\n"
	fmt.Println(commit)
	log.count = log.count + 1
	*log.index += 1
	_, err := writer.WriteString(commit)
	err = writer.Flush()
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

// ImportLog is read log from others and do it
func (log *Inline) ImportLog(index int32, logs []string) error {
	log.Lock()
	defer log.Unlock()
	writer := bufio.NewWriter(log.f)
	if *log.index > index {
		fmt.Printf("should redo in import log log.index :%v, index: %v\n", *log.index, index)
		// should redo
		// simple implement, we will seek the first redo line from head
		_, _ = log.f.Seek(0, io.SeekStart)
		reader := bufio.NewReader(log.f)

		for {
			l, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			items := strings.Fields(l)
			in, err := strconv.ParseInt(items[len(items)-1], 10, 32)
			if err != nil {
				return err
			}
			if int32(in) == index {
				// the next line should redo
				fmt.Printf("log items :%v\n", items)
				break
			}
		}

		for {
			l, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			items := strings.Fields(l)
			fmt.Printf("add redo items: %v\n", items)
			/*  The log is format like this
			Delete : D {key} {value} {index}
			Put: P {key} {value} {oldValue} {index}
			*/
			switch items[0] {
			case "P" : {
				oldValue := items[3]
				key := items[1]
				value := items[2]
				logs = append(logs, "P" + " " + key + " " + oldValue + " " + value + " " + strconv.Itoa(int(index)))
			}
			case "D" : {
				oldValue := items[2]
				key := items[1]
				logs = append(logs, "D" + " " + key + " " + oldValue + " " + strconv.Itoa(int(index)))
			}
			default: {
				return errors.New("invalid log")
			}
			}
		}

	}
	for _, val := range logs {
		_, err := writer.WriteString(val + "\n")
		if err != nil {
			// TODO need rollback
			return err
		}
		fmt.Printf("import log write %s\n", val)
	}
	err := writer.Flush()
	*log.index = index
	log.f.Seek(0, io.SeekEnd)
	return err

}

// LoadLog from file
func (log *Inline) LoadLog(index int32) ([]string, int32, error) {
	if index == 6 {
		fmt.Printf("log.index : %v index: %v\n", *log.index, index)
	}
	if index > *log.index {
		// should redo
		fmt.Println("should redo")
		return nil, *log.index, nil
	}
	if index == *log.index {
		return nil, *log.index, nil
	}
	reader := bufio.NewReader(log.f)
	var lines []string
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
		//*log.index = int32(newIndex)
		return lines, int32(newIndex), nil
	}
	return lines, index, nil
}

func (log *Inline) GetIndex() *int32 {
	return log.index
}

func (log *Inline) SetIndex(index int32) {
	*log.index = index
}

// Destruct fd
func (log *Inline) Destruct() error {
	err := log.f.Close()
	return err
}
