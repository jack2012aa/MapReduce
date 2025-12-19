package coordinator

import (
	"MapReduce/src/protocol"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	lastHeartbeats map[string]time.Time
	isAlive        bool
	timeout        time.Duration
	am             *assignmentManager
	mu             sync.Mutex
}

func MakeCoordinatorDaemon() *Coordinator {
	c := Coordinator{}
	c.lastHeartbeats = make(map[string]time.Time)
	c.isAlive = true
	c.timeout = time.Second * 10
	c.am = newAssignmentManager()

	c.server()
	return &c
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := MakeCoordinatorDaemon()
	c.am.addTaskFromFiles(files, nReduce)
	return c
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
				c.am.maybeUnassign(s)
			}
		}
	}
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
	c.lastHeartbeats[args.Source] = time.Now()
	return nil
}

func (c *Coordinator) AskAssignment(args protocol.AskAssignmentRequest, reply *protocol.AskAssignmentResponse) error {
	a := c.am.assignTask(args.Source)
	if a == nil {
		reply.IsEmpty = true
	} else {
		reply.A = *a
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

func (c *Coordinator) server() {
	rpc.Register(c)

	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}
