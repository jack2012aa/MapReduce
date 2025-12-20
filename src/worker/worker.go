package worker

import (
	"MapReduce/src/protocol"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerDaemon struct {
	host            *string
	port            int
	coordinatorAddr *string
	client          *rpc.Client
	run             int
	isClosed        atomic.Bool
	mu              sync.Mutex
	mapF            func(string, string) []KeyValue
	reduceF         func(string, []string) string
	dir             string
}

type KeyValue struct {
	Key   string
	Value string
}

type WorkerDaemonOption func(*WorkerDaemon)

func WithMapF(f func(string, string) []KeyValue) WorkerDaemonOption {
	return func(wd *WorkerDaemon) {
		wd.mapF = f
	}
}

func WithReduceF(f func(string, []string) string) WorkerDaemonOption {
	return func(wd *WorkerDaemon) {
		wd.reduceF = f
	}
}

func WithRun(n int) WorkerDaemonOption {
	return func(wd *WorkerDaemon) {
		wd.run = n
	}
}

func WithBaseDir(dir string) WorkerDaemonOption {
	return func(wd *WorkerDaemon) {
		wd.dir = dir
	}
}

func MakeWorkerDaemon(coordinatorAddr string, opts ...WorkerDaemonOption) *WorkerDaemon {
	wd := &WorkerDaemon{coordinatorAddr: &coordinatorAddr, run: -1, dir: "."}
	for _, opt := range opts {
		opt(wd)
	}
	return wd
}

func (wd *WorkerDaemon) startFileServer() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("start file server error:", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	fs := http.FileServer(http.Dir(wd.dir))
	go func() {
		if err := http.Serve(listener, fs); err != nil {
			log.Printf("http serve error: %s", err)
		}
	}()
	wd.port = port
}

func (wd *WorkerDaemon) setAddr() {
	conn, err := net.Dial("udp", *wd.coordinatorAddr)
	if err != nil {
		log.Fatal("set address error:", err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	host := localAddr.IP.String()
	wd.host = &host
}

func (wd *WorkerDaemon) register() {
	id := wd.addr()
	q := protocol.RegisterWorkerRequest{Addr: &id}
	p := &protocol.RegisterWorkerResponse{}
	err := wd.client.Call("Coordinator.RegisterWorker", q, p)
	if err != nil {
		log.Fatal("register worker error:", err)
	}
}

func (wd *WorkerDaemon) heartbeat() {
	addr := wd.addr()
	q := protocol.HeartBeatRequest{Source: &addr}
	p := &protocol.HeartBeatResponse{}
	for !wd.isClosed.Load() {
		err := wd.client.Call("Coordinator.Heartbeat", q, p)
		if err != nil {
			log.Println("heartbeat error:", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (wd *WorkerDaemon) addr() string {
	return fmt.Sprintf("%v:%v", *wd.host, wd.port)
}

func (wd *WorkerDaemon) askAssignment() protocol.Assignment {
	addr := wd.addr()
	q := protocol.AskAssignmentRequest{Source: &addr}
	p := &protocol.AskAssignmentResponse{}
	err := wd.client.Call("Coordinator.AskAssignment", q, p)
	if err != nil {
		log.Fatal("assign worker error:", err)
	}
	return p.A
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func (wd *WorkerDaemon) loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func (wd *WorkerDaemon) reportError(err error) {
	q := protocol.ReportErrorRequest{Error: err}
	p := &protocol.ReportErrorResponse{}
	wd.client.Call("Coordinator.ReportError", q, p)
}

func (wd *WorkerDaemon) ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wd *WorkerDaemon) workAt(a protocol.Assignment) {
	dir := fmt.Sprintf("%v", a.TaskID())
	err := os.Mkdir(dir, 0777)
	if err != nil {
		wd.reportError(err)
		return
	}
	_, isMap := a.(*protocol.MapAssignment)
	if wd.mapF != nil && isMap {
		a.SetF(wd.mapF)
	} else if wd.reduceF != nil && isMap {
		a.SetF(wd.reduceF)
	}
	outputs, err := a.Execute(dir)
	if err != nil {
		wd.reportError(err)
		return
	}
	addr := wd.addr()
	if isMap {
		for i, p := range outputs {
			outputs[i] = fmt.Sprintf("http://%v%v", addr, p)
		}
	}
	outputsP := make(map[int]*string, len(outputs))
	for i, output := range outputs {
		outputsP[i] = &output
	}
	if outputs != nil {
		q := protocol.FinishAssignmentRequest{Source: &addr, Outputs: outputsP}
		p := &protocol.FinishAssignmentResponse{}
		wd.client.Call("Coordinator.FinishAssignment", q, p)
	}
}

func (wd *WorkerDaemon) Start() {
	client, err := rpc.Dial("tcp", *wd.coordinatorAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	wd.client = client
	wd.startFileServer()
	wd.setAddr()
	wd.register()
	go wd.heartbeat()
	for i := 0; wd.run == -1 || i < wd.run; {
		a := wd.askAssignment()
		if a != nil {
			wd.workAt(a)
			i++
		} else {
			time.Sleep(time.Second)
		}
	}
}

func (wd *WorkerDaemon) Close() {
	wd.isClosed.Store(true)
}

func Worker(coordinatorAddr string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	wd := MakeWorkerDaemon(coordinatorAddr, WithMapF(mapf), WithReduceF(reducef), WithRun(1))
	wd.Start()
	wd.Close()
}
