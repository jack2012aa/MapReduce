package coordinator

import (
	"MapReduce/src/protocol"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type assignmentManager struct {
	ready   chan *protocol.Assignment
	wToA    map[string]*protocol.Assignment
	tempToA map[string]*protocol.Assignment
	idToT   map[protocol.ID]*task
	mu      sync.Mutex
}

func newAssignmentManager() *assignmentManager {
	am := &assignmentManager{}
	chSize := 100
	am.ready = make(chan *protocol.Assignment, chSize)
	am.wToA = make(map[string]*protocol.Assignment)
	am.tempToA = make(map[string]*protocol.Assignment)
	am.idToT = make(map[protocol.ID]*task)
	return am
}

func (am *assignmentManager) splitTask(r protocol.StartTaskRequest, id protocol.ID) {
	am.mu.Lock()
	t := makeTask()
	am.idToT[id] = t
	am.mu.Unlock()

	t.mu.Lock()
	t.id = id
	t.nReduce = r.NReduce
	info, err := os.Stat(r.Input)
	if err != nil {
		t.kill()
		log.Println("Err when getting task info:", err)
		t.mu.Unlock()
		return
	}

	sBytes := info.Size()
	newAssignments := make([]*protocol.Assignment, 0)
	for i := int64(0); i < sBytes; i += r.MapSize {
		a := &protocol.Assignment{
			Map: protocol.MapAssignment{
				Input:  r.Input,
				Offset: i,
				Length: r.MapSize,
				Plugin: r.Plugin,
			},
			TaskID: t.id,
		}
		t.assignments = append(t.assignments, a)
		newAssignments = append(newAssignments, a)
		t.nMap++
	}
	t.mu.Unlock()

	// Only after everything is done, jobs can be assigned
	go func() {
		for _, a := range newAssignments {
			am.ready <- a
		}
	}()
}

func (am *assignmentManager) addTask(r protocol.StartTaskRequest) (protocol.ID, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return protocol.ID{}, err
	}
	go am.splitTask(r, protocol.ID(id))
	return protocol.ID(id), err
}

func (am *assignmentManager) addTaskFromFiles(files []string, nReduce int) (protocol.ID, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return protocol.ID{}, err
	}
	go func() {
		am.mu.Lock()
		t := makeTask()
		am.idToT[t.id] = t
		am.mu.Unlock()

		t.mu.Lock()
		t.id = protocol.ID(id)
		t.nMap = len(files)
		t.nReduce = nReduce
		newAssignments := make([]*protocol.Assignment, 0)
		for _, file := range files {
			a := &protocol.Assignment{
				Map: protocol.MapAssignment{
					Input:  file,
					Offset: 0,
					Length: -1,
					Plugin: "",
				},
				TaskID: t.id,
			}
			t.assignments = append(t.assignments, a)
			newAssignments = append(newAssignments, a)
		}
		t.mu.Unlock()

		go func() {
			for _, a := range newAssignments {
				am.ready <- a
			}
		}()
	}()
	return protocol.ID(id), err
}

func (am *assignmentManager) maybeUnassign(worker string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if a, ok := am.wToA[worker]; ok {
		delete(am.wToA, worker)
		go func() {
			am.ready <- a
		}()
	}
}

func (am *assignmentManager) isTaskDone(tID protocol.ID) (bool, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	t, ok := am.idToT[tID]
	if !ok {
		return false, errors.New("task not found")
	}
	return t.isDone(), nil
}

func (am *assignmentManager) isTaskDead(tID protocol.ID) (bool, error) {
	am.mu.Lock()
	defer am.mu.Unlock()
	t, ok := am.idToT[tID]
	if !ok {
		return false, errors.New("task not found")
	}
	return t.isDead(), nil
}

func (am *assignmentManager) assignTask(worker string) *protocol.Assignment {
	for {
		a, ok := <-am.ready
		if !ok {
			return nil
		}

		am.mu.Lock()
		t, ok := am.idToT[a.TaskID]
		if !ok || t.isDead() || (a.IsMap() && t.readyToReduce()) {
			am.mu.Unlock()
			continue
		}

		am.wToA[worker] = a
		am.mu.Unlock()

		return a
	}
}

func (am *assignmentManager) finishMap(m *protocol.MapAssignment, t *task, outputs map[int]string, id string) {
	am.mu.Lock()
	mapA := &protocol.Assignment{Map: *m, TaskID: t.id}
	for _, file := range outputs {
		am.tempToA[file] = mapA
	}
	am.mu.Unlock()
	if t.finishMap(outputs, id) {
		t.mu.Lock()
		newAssignments := make([]*protocol.Assignment, 0)
		for hash, files := range t.temps {
			a := &protocol.Assignment{
				Reduce: protocol.ReduceAssignment{
					Input:  files,
					Hash:   strconv.Itoa(hash),
					Plugin: m.Plugin,
				},
				TaskID: t.id,
			}
			t.assignments = append(t.assignments, a)
			newAssignments = append(newAssignments, a)
		}
		t.mu.Unlock()
		go func() {
			for _, a := range newAssignments {
				am.ready <- a
			}
		}()
	}
}

func (am *assignmentManager) finishReduce(m *protocol.ReduceAssignment, t *task, id string) {
	t.finishReduce(id)
}

func (am *assignmentManager) finishAssignment(worker string, outputs map[int]string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	a, ok := am.wToA[worker]
	if !ok {
		log.Println("assignment worker not found")
		return
	}
	t, ok := am.idToT[a.TaskID]
	if !ok {
		log.Println("assignment task not found")
		return
	}
	if a.IsMap() {
		go am.finishMap(&a.Map, t, outputs, a.ID())
	} else {
		go am.finishReduce(&a.Reduce, t, a.ID())
	}

	delete(am.wToA, worker)
}

// Redo the MapAssignment whose result is temp.
func (am *assignmentManager) redoMapOfTemp(temp string) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	a, ok := am.tempToA[temp]
	if !ok {
		return errors.New("the task is already redo")
	}
	delete(am.tempToA, temp)
	t := am.idToT[a.TaskID]
	t.stateRedoMap()
	return nil
}

// Redo all assignments of a task
func (am *assignmentManager) redoTask(id protocol.ID) error {
	am.mu.Lock()
	t, ok := am.idToT[id]
	am.mu.Unlock()
	if !ok {
		return errors.New("task not found")
	}

}

// Internal task state
const (
	mapping = iota
	reducing
	done
	dead
)

type task struct {
	id          protocol.ID
	assignments []*protocol.Assignment
	doneAIDs    map[string]bool
	nMap        int
	doneMap     int
	nReduce     int
	doneReduce  int
	temps       map[int][]string
	mu          sync.Mutex
	state       int
}

func makeTask() *task {
	return &task{
		assignments: make([]*protocol.Assignment, 0),
		doneAIDs:    make(map[string]bool),
		temps:       make(map[int][]string),
	}
}

func (t *task) kill() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = dead
}

func (t *task) isDead() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == dead
}

// proceed is not thread-safe.
func (t *task) proceed() {
	t.state++
}

// goBack is not thread-safe
func (t *task) goBack() {
	t.state--
}

func (t *task) finishMap(outputs map[int]string, id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == mapping && !t.doneAIDs[id] {
		t.doneAIDs[id] = true
		for hash, file := range outputs {
			t.temps[hash] = append(t.temps[hash], file)
		}
		if t.doneMap == t.nMap {
			t.proceed()
			return true
		}
	}
	return false
}

func (t *task) finishReduce(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == reducing {
		t.doneAIDs[id] = true
		t.doneReduce++
		if t.doneReduce == t.nReduce {
			t.proceed()
			return true
		}
	}
	return false
}

func (t *task) readyToReduce() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nMap == t.doneMap && t.state == reducing
}

func (t *task) isDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == done
}

func (t *task) stateRedoMap(a *protocol.Assignment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.doneAIDs, a.ID())
	t.doneMap--
	if t.state == reducing {
		t.goBack()
	}
}
