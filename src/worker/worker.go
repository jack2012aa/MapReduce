package worker

import (
	"MapReduce/src/protocol"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
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
	mapF            func(string, string) []protocol.KeyValue
	reduceF         func(string, []string) string
	dir             string
}

type WorkerDaemonOption func(*WorkerDaemon)

func WithMapF(f func(string, string) []protocol.KeyValue) WorkerDaemonOption {
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
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir(wd.dir)))

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("start file server error:", err)
	}
	wd.port = listener.Addr().(*net.TCPAddr).Port
	go func() {
		if err := http.Serve(listener, mux); err != nil {
			log.Printf("http serve error: %s", err)
		}
	}()
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
		err := wd.client.Call("Coordinator.HeartBeat", q, p)
		if err != nil {
			log.Println("heartbeat error:", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (wd *WorkerDaemon) addr() string {
	return fmt.Sprintf("%v:%v", *wd.host, wd.port)
}

func (wd *WorkerDaemon) askAssignment() (protocol.Assignment, error) {
	addr := wd.addr()
	q := protocol.AskAssignmentRequest{Source: &addr}
	p := &protocol.AskAssignmentResponse{}
	err := wd.client.Call("Coordinator.AskAssignment", q, p)
	if err != nil {
		return nil, errors.New("assign worker error: " + err.Error())
	}
	return p.A, nil
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func (wd *WorkerDaemon) loadPlugin(filename string) (func(string, string) []protocol.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []protocol.KeyValue)
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

func (wd *WorkerDaemon) workAt(a protocol.Assignment) {
	taskDir := fmt.Sprintf("%v", a.TaskID())
	absTaskDir := filepath.Join(wd.dir, taskDir)
	_, err := os.Stat(absTaskDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(absTaskDir, 0777)
	}
	_, isMap := a.(*protocol.MapAssignment)
	if wd.mapF != nil && isMap {
		a.SetF(wd.mapF)
	} else if wd.reduceF != nil && !isMap {
		a.SetF(wd.reduceF)
	}
	outputs, err := a.Execute(absTaskDir)
	if err != nil {
		wd.reportError(err)
		return
	}
	addr := wd.addr()
	if isMap {
		for i, p := range outputs {
			p = path.Join(taskDir, p)
			outputs[i] = fmt.Sprintf("http://%v/%v", addr, p)
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
	client, err := rpc.DialHTTP("tcp", *wd.coordinatorAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	wd.client = client
	wd.startFileServer()
	wd.setAddr()
	wd.register()
	go wd.heartbeat()
	for i := 0; wd.run == -1 || i < wd.run; {
		a, err := wd.askAssignment()
		if err != nil {
			wd.reportError(err)
		} else if a != nil {
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

func Worker(coordinatorAddr string, mapf func(string, string) []protocol.KeyValue, reducef func(string, []string) string) {
	wd := MakeWorkerDaemon(coordinatorAddr, WithMapF(mapf), WithReduceF(reducef), WithRun(1))
	wd.Start()
	wd.Close()
}
