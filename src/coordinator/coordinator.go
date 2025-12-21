package coordinator

import (
	"MapReduce/src/protocol"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	lastHeartbeats map[string]time.Time
	isClosed       atomic.Bool
	srv            *http.Server
	timeout        time.Duration
	am             *assignmentManager
	mu             sync.Mutex
}

func MakeCoordinatorDaemon() (*Coordinator, string) {
	c := Coordinator{
		lastHeartbeats: make(map[string]time.Time),
		isClosed:       atomic.Bool{},
		timeout:        time.Second * 10,
		am:             newAssignmentManager(),
	}

	addr := c.server()
	return &c, addr
}

func MakeCoordinator(files []string, nReduce int) (*Coordinator, string) {
	c, addr := MakeCoordinatorDaemon()
	var filesP []*string
	for _, file := range files {
		filesP = append(filesP, &file)
	}
	c.am.addTaskFromFiles(filesP, nReduce)
	return c, addr
}

func (c *Coordinator) StartTask(args protocol.StartTaskRequest, reply *protocol.StartTaskResponse) error {

	id, err := c.am.addTask(args)
	if err != nil {
		return err
	}
	reply.TaskID = id

	return nil
}

func (c *Coordinator) checkHeartBeat() {
	for {
		for s, t := range c.lastHeartbeats {
			now := time.Now()
			delta := now.Sub(t)
			if delta > c.timeout {
				c.am.maybeUnassign(&s)
			}
			delete(c.lastHeartbeats, s)
		}
	}
}

func (c *Coordinator) RegisterWorker(args protocol.RegisterWorkerRequest, reply *protocol.RegisterWorkerResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastHeartbeats[*args.Addr] = time.Now()
	return nil
}

func (c *Coordinator) AskTask(args protocol.AskTaskRequest, reply *protocol.AskTaskResponse) error {
	if s, err := c.am.isTaskDone(args.TaskID); err != nil {
		return err
	} else if s {
		reply.State = protocol.TaskDone
		return nil
	}

	if s, err := c.am.isTaskDead(args.TaskID); err != nil {
		return err
	} else if s {
		reply.State = protocol.TaskDead
		return nil
	}

	reply.State = protocol.TaskWorking
	return nil
}

func (c *Coordinator) HeartBeat(args protocol.HeartBeatRequest, reply *protocol.HeartBeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastHeartbeats[*args.Source] = time.Now()
	return nil
}

func (c *Coordinator) AskAssignment(args protocol.AskAssignmentRequest, reply *protocol.AskAssignmentResponse) error {
	a := c.am.assignTask(*args.Source)
	if a == nil {
		reply.IsEmpty = true
	} else {
		reply.A = a
	}
	return nil
}

func (c *Coordinator) FinishAssignment(args protocol.FinishAssignmentRequest, reply *protocol.FinishAssignmentResponse) error {
	c.am.finishAssignment(args.Source, args.Outputs)
	return nil
}

func (c *Coordinator) DeadTemp(args protocol.DeadTempRequest, reply *protocol.DeadTempResponse) error {
	return c.am.redoTemp(args.Source, args.Temp)
}

func (c *Coordinator) GetResult(args protocol.GetResultRequest, reply *protocol.GetResultResponse) error {
	results, err := c.am.getResult(args.TaskID)
	if err != nil {
		return err
	}
	reply.Output = results
	return nil
}

func (c *Coordinator) ReportError(args protocol.ReportErrorRequest, reply *protocol.ReportErrorResponse) error {
	c.am.maybeUnassign(args.Source)
	log.Printf("Worker %v fails because of %v", args.Source, args.Error)
	return nil
}

func (c *Coordinator) server() string {
	rpcServer := rpc.NewServer()
	rpcServer.Register(c)
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	mux.Handle(rpc.DefaultDebugPath, rpcServer)

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	addr := l.Addr().String()
	log.Printf("Coordinator listening at %s", addr)

	c.srv = &http.Server{Handler: mux}
	go func() {
		if err := c.srv.Serve(l); err != nil {
			log.Printf("Coordinator failed to serve: %v", err)
		}
	}()
	return addr
}

func (c *Coordinator) Close() {
	c.isClosed.Store(true)
	c.srv.Close()
}
