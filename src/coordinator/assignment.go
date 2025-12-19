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
	workingTasks []*task
	wToA         map[string]*protocol.Assignment
	idToT        map[protocol.ID]*task
	mu           sync.Mutex
}

func newAssignmentManager() *assignmentManager {
	am := &assignmentManager{}
	am.workingTasks = []*task{}
	am.wToA = make(map[string]*protocol.Assignment)
	am.idToT = make(map[protocol.ID]*task)
	return am
}

func (am *assignmentManager) addTask(r protocol.StartTaskRequest) (protocol.ID, error) {
	t, err := makeTask([]string{r.Input}, r.NReduce, r.Plugin, withMapSize(r.MapSize))
	if err != nil {
		return protocol.ID{}, err
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	am.workingTasks = append(am.workingTasks, t)
	return t.id, nil
}

func (am *assignmentManager) addTaskFromFiles(inputs []string, nReduce int) (protocol.ID, error) {
	t, err := makeTask(inputs, nReduce, "")
	if err != nil {
		return protocol.ID{}, err
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	am.workingTasks = append(am.workingTasks, t)
	return t.id, nil
}

func (am *assignmentManager) maybeUnassign(worker string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	a, ok := am.wToA[worker]
	if ok {
		delete(am.wToA, worker)
	}
	t := am.idToT[a.TaskID]
	t.cancel(a)
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
	am.mu.Lock()
	defer am.mu.Unlock()
	for _, t := range am.workingTasks {
		if a := t.assign(); a != nil {
			am.wToA[worker] = a
			return a
		}
	}
	return nil
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
	delete(am.wToA, worker)
	go t.finish(a, outputs)
}

func (am *assignmentManager) redoTemp(worker string, path string) error {
	am.mu.Lock()
	defer am.mu.Unlock()
	a, ok := am.wToA[worker]
	t := am.idToT[a.TaskID]
	if !ok {
		return errors.New("assignment worker not found")
	}
	return t.redoTemp(a, path)
}

const (
	// Internal task state
	tMapping = iota
	tReducing
	tDone
	tDead

	// Internal assignment state
	aReady
	aWorking
	aBlocked
	aDone
)

type aWrapper struct {
	a     *protocol.Assignment
	state int
}

type taskOption func(*task)

func withMapSize(s int64) taskOption {
	return func(t *task) {
		t.mSize = s
	}
}

type task struct {
	mSize      int64
	nMap       int
	doneMap    int
	nReduce    int
	doneReduce int
	plugin     string
	id         protocol.ID
	mA         map[string]*aWrapper
	rA         map[string]*aWrapper
	inputs     []string
	temps      map[int][]string
	results    map[int][]string
	tempsToAW  map[string]*aWrapper
	mu         sync.Mutex
	state      int
}

// makeTask creates a new task structure.
//
// If len(inputs) = 1, the input file is split to appropriate size
//
// If len(inputs) > 1, map assignments are created for each input file
func makeTask(inputs []string, nReduce int, plugin string, opts ...taskOption) (*task, error) {
	t := &task{
		mSize:     32 * 1024 * 1024,
		nReduce:   nReduce,
		plugin:    plugin,
		id:        protocol.ID(uuid.New()),
		mA:        make(map[string]*aWrapper, 10),
		rA:        make(map[string]*aWrapper, nReduce),
		temps:     make(map[int][]string),
		results:   make(map[int][]string),
		tempsToAW: make(map[string]*aWrapper),
	}

	for _, opt := range opts {
		opt(t)
	}

	if len(inputs) == 1 {
		err := t.splitAndSetMA(inputs[0], plugin)
		if err != nil {
			return nil, err
		}
	} else if len(inputs) > 1 {
		t.setMA(inputs, plugin)
	} else {
		return nil, errors.New("inputs are required")
	}

	return t, nil
}

// This function is not thread-safe and should only be called in the constructor
func (t *task) splitAndSetMA(input string, plugin string) error {
	info, err := os.Stat(input)
	if err != nil {
		t.kill()
		return errors.New("err when getting task info")
	}

	sBytes := info.Size()
	for i := int64(0); i < sBytes; i += t.mSize {
		ma := protocol.MapAssignment{
			Input:  input,
			Offset: i,
			Length: t.mSize,
			Plugin: plugin,
		}
		aw := &aWrapper{a: &protocol.Assignment{Map: ma, TaskID: t.id}, state: aReady}
		t.mA[aw.a.ID()] = aw
	}
	t.nMap = len(t.mA)
	return nil
}

// This function is not thread-safe and should only be called in the constructor
func (t *task) setMA(inputs []string, plugin string) {
	for _, input := range inputs {
		ma := protocol.MapAssignment{
			Input:  input,
			Offset: 0,
			Length: -1,
			Plugin: plugin,
		}
		aw := &aWrapper{a: &protocol.Assignment{Map: ma, TaskID: t.id}, state: aReady}
		t.mA[aw.a.ID()] = aw
	}
	t.nMap = len(t.mA)
}

func (t *task) kill() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = tDead
}

func (t *task) cancel(a *protocol.Assignment) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if a.IsMap() {
		wa, ok := t.mA[a.ID()]
		if ok {
			wa.state = aReady
		}
	} else {
		wa, ok := t.rA[a.ID()]
		if ok {
			wa.state = aReady
		}
	}
}

func (t *task) isDead() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == tDead
}

func (t *task) assign() *protocol.Assignment {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == tMapping {
		for _, aw := range t.mA {
			if aw.state == aReady {
				aw.state = aWorking
				return aw.a
			}
		}
	} else if t.state == tReducing {
		for _, aw := range t.rA {
			if aw.state == aReady {
				aw.state = aWorking
				return aw.a
			}
		}
	}
	return nil
}

func (t *task) finish(a *protocol.Assignment, outputs map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == tMapping {
		aw, ok := t.mA[a.ID()]
		if ok {
			for hash, path := range outputs {
				t.tempsToAW[path] = aw
				t.temps[hash] = append(t.temps[hash], path)
			}
			aw.state = aDone
			t.doneMap++
		}
	} else if t.state == tReducing {
		aw, ok := t.rA[a.ID()]
		if ok {
			for hash, path := range outputs {
				t.results[hash] = append(t.results[hash], path)
			}
			aw.state = aDone
			t.doneReduce++
		}
	}
	t.proceed()
}

// proceed is not thread-safe.
func (t *task) proceed() {
	if t.state == tMapping && t.doneMap == t.nMap {
		t.state = tReducing

		if len(t.rA) == 0 {
			for hash, paths := range t.temps {
				ra := protocol.ReduceAssignment{
					Inputs: paths,
					Hash:   strconv.Itoa(hash),
					Plugin: t.plugin,
				}
				aw := &aWrapper{a: &protocol.Assignment{Reduce: ra, TaskID: t.id}, state: aReady}
				t.rA[aw.a.ID()] = aw
			}
		} else {
			for _, aw := range t.rA {
				if aw.state == aBlocked {
					h, err := strconv.Atoi(aw.a.Reduce.Hash)
					if err != nil {
						log.Println("invalid hash")
						continue
					}
					aw.a.Reduce.Inputs = t.temps[h]
					aw.state = aReady
				}
			}
		}

	} else if t.state == tReducing && t.doneReduce == t.nReduce {
		t.state = tDone
	}
}

// goBack is not thread-safe
func (t *task) goBack() {
	t.state--
}

func (t *task) isDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == tDone
}

func (t *task) redoTemp(ra *protocol.Assignment, path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	maw, _ := t.tempsToAW[path]
	removed := make(map[string]bool)
	for temp, other := range t.tempsToAW {
		if maw == other {
			removed[temp] = true
		}
	}
	for h, l := range t.temps {
		var temps []string
		for _, temp := range l {
			if !removed[temp] {
				temps = append(temps, temp)
			}
		}
		t.temps[h] = temps
	}
	t.doneMap--
	maw.state = aReady
	raw := t.rA[ra.ID()]
	raw.state = aBlocked
	t.state = tMapping
	return nil
}
