package protocol

import (
	"cmp"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"

	"github.com/google/uuid"
)

func init() {
	gob.Register(&MapAssignment{})
	gob.Register(&ReduceAssignment{})
}

type KeyValue struct {
	Key   string
	Value string
}

type ID uuid.UUID

type Assignment interface {
	Execute(dir string) (outputs map[int]string, err error)
	SetF(any) error
	TaskID() ID
	AssignmentID() string
}

type MapAssignment struct {
	Input    *string
	Offset   int64
	Length   int64
	Function func(string, string) []KeyValue
	NReduce  int
	TID      ID
}

func (a *MapAssignment) ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetOutBoundIp(targetAddr string) (string, error) {
	conn, err := net.Dial("udp", targetAddr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// Execute creates a new directory and maps/reduces the input to some outputs.
//
// If it is a reduce assignment, may return FileAccessError
func (a *MapAssignment) Execute(dir string) (outputs map[int]string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("MapF panic: %v\nStack Trace:\n%s", r, debug.Stack())
			outputs = nil
		}
	}()
	if a.Function == nil {
		return nil, errors.New("no executable function")
	}

	input, err := os.Open(*a.Input)
	if err != nil {
		return nil, err
	}

	_, err = input.Seek(a.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, a.Length)
	_, err = io.ReadFull(input, buffer)
	if err != nil {
		return nil, err
	}
	contents := string(buffer)
	input.Close()

	// map
	kvs := a.Function(*a.Input, contents)

	// hash
	encoders := make([]*json.Encoder, a.NReduce)
	outputFiles := make([]*os.File, a.NReduce)
	outputs = make(map[int]string, a.NReduce)
	err = os.Mkdir(dir, 0777)
	if err != nil {
		return nil, err
	}

	for i := 0; i < a.NReduce; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("mr-tmp-%v", i))
		output, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		outputFiles[i] = output
		encoders[i] = json.NewEncoder(output)
	}

	for _, kv := range kvs {
		h := a.ihash(kv.Key) % a.NReduce
		err = encoders[h].Encode(&kv)
		if err != nil {
			return nil, err
		}
	}

	for i, file := range outputFiles {
		file.Close()
		filename := fmt.Sprintf("/%v/mr-%v-%v", dir, a.TaskID(), i)
		os.Rename(file.Name(), filename)
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		outputs[i] = fmt.Sprintf("%s/%s", wd, filename)
	}

	return outputs, nil
}

func (a *MapAssignment) SetF(f any) error {
	mapF, ok := f.(func(string, string) []KeyValue)
	if !ok {
		return errors.New("mapF function must be func(string, string) []KeyValue")
	}
	a.Function = mapF
	return nil
}

func (a *MapAssignment) TaskID() ID {
	return a.TID
}

func (a *MapAssignment) AssignmentID() string {
	return fmt.Sprintf("t_%v_i_%v_o_%v", a.TID, a.Input, a.Offset)
}

type FileAccessError struct {
	Addr *string
}

func (err *FileAccessError) Error() string {
	return fmt.Sprintf("cannot access %v", *err.Addr)
}

type ReduceAssignment struct {
	Inputs   []*string
	Hash     *string
	Function func(string, []string) string
	TID      ID
}

func (a *ReduceAssignment) downloadAndDecode(url string) ([]KeyValue, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, &FileAccessError{&url}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, &FileAccessError{&url}
	}

	dec := json.NewDecoder(resp.Body)
	var kvs []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		kvs = append(kvs, kv)
	}

	return kvs, nil
}

// Execute creates a new directory and maps/reduces the input to some outputs.
//
// If it is a reduce assignment, may return FileAccessError
func (a *ReduceAssignment) Execute(dir string) (outputs map[int]string, err error) {
	var allKVs []KeyValue

	for _, url := range a.Inputs {
		kvs, err := a.downloadAndDecode(*url)
		if err != nil {
			return nil, err
		}
		allKVs = append(allKVs, kvs...)
	}

	slices.SortFunc(allKVs, func(a, b KeyValue) int {
		return cmp.Compare(a.Key, b.Key)
	})

	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}
	filename := filepath.Join(dir, fmt.Sprintf("mr-out-%v", a.Hash))
	output, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer output.Close()

	for i := 0; i < len(allKVs); {
		var values []string
		key := allKVs[i].Key
		for j := i; j < len(allKVs) && allKVs[j].Key == key; j++ {
			values = append(values, allKVs[j].Value)
		}
		r := a.Function(key, values)
		fmt.Fprintf(output, "%v %v\n", key, r)
		i += len(values)
	}
	h, err := strconv.Atoi(*a.Hash)
	if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(output.Name())
	if err != nil {
		return nil, err
	}
	outputs[h] = path
	return outputs, nil
}

func (a *ReduceAssignment) SetF(f any) error {
	reduceF, ok := f.(func(string, []string) string)
	if !ok {
		return errors.New("reduceF function must be func(string, []string) string")
	}
	a.Function = reduceF
	return nil
}

func (a *ReduceAssignment) TaskID() ID {
	return a.TID
}

func (a *ReduceAssignment) AssignmentID() string {
	return fmt.Sprintf("t_%v_h_%v", a.TID, a.Hash)
}
