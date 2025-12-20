package coordinator

import (
	"MapReduce/src/protocol"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestMakeTask(t *testing.T) {
	tempDir := t.TempDir()
	file1 := filepath.Join(tempDir, "file1")
	file2 := filepath.Join(tempDir, "file2")
	content := []byte("hello world")
	err := os.WriteFile(file1, content, 0644)
	err = os.WriteFile(file2, content, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var mSize int64 = 1
	nMap := len(content) / int(mSize)
	mapF := func(k string, c string) []protocol.KeyValue { return nil }
	reduceF := func(k string, v []string) string { return "" }

	tests := []struct {
		name     string
		inputs   []*string
		nReduce  int
		opts     []taskOption
		expMA    int
		expRA    int
		expMSize int64
	}{
		{"Single File", []*string{&file1}, 10, []taskOption{}, 1, 0, 32 * 1024 * 1024},
		{"Multiple Files", []*string{&file1, &file2}, 10, []taskOption{}, 2, 0, 32 * 1024 * 1024},
		{"With", []*string{&file1}, 10, []taskOption{withMapSize(mSize), withMapF(mapF), withReduceF(reduceF)}, nMap, 0, mSize},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task, err := makeTask(test.inputs, test.nReduce, test.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if len(task.mA) != test.expMA {
				t.Errorf("got len mA %d, want %d", len(task.mA), test.expMA)
			}
			if task.nMap != test.expMA {
				t.Errorf("got nMap %d, want %d", task.nMap, test.expMA)
			}
			if task.nReduce != test.nReduce {
				t.Errorf("got nReduce %d, want %d", task.nReduce, test.nReduce)
			}
			if task.mapF != nil && len(test.opts) == 0 {
				t.Errorf("got mapF, want nil")
			}
			if task.reduceF != nil && len(test.opts) == 0 {
				t.Errorf("got reduceF, want nil")
			}
			if task.mSize != int64(test.expMSize) {
				t.Errorf("got mSize %d, want %d", task.mSize, test.expMSize)
			}
			if len(task.rA) != 0 {
				t.Errorf("got rA %d, want %d", len(task.rA), 0)
			}
			if len(task.temps) != 0 {
				t.Errorf("got temps %d, want %d", len(task.temps), 0)
			}
			if len(task.results) != 0 {
				t.Errorf("got results %d, want %d", len(task.results), 0)
			}
			if len(task.tempsToAW) != 0 {
				t.Errorf("got tempsToAW %d, want %d", len(task.tempsToAW), 0)
			}
		})
	}
}

func getTestTask(t *testing.T, i int) *task {
	tempDir := t.TempDir()
	file := filepath.Join(tempDir, "file"+strconv.Itoa(i))
	content := []byte("hello world")
	err := os.WriteFile(file, content, 0644)
	if err != nil {
		t.Fatal(err)
	}

	task, err := makeTask([]*string{&file}, 1, withMapSize(int64(1)))
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestKill(t *testing.T) {
	task := getTestTask(t, 0)
	task.kill()
	if !task.isDead() {
		t.Errorf("task should be dead")
	}
}

func TestAssign(t *testing.T) {
	taskM := getTestTask(t, 0)
	taskR := getTestTask(t, 1)
	taskD := getTestTask(t, 2)
	taskE := getTestTask(t, 3)
	taskDead := getTestTask(t, 4)
	taskR.state = tReducing
	taskR.rA["1"] = &aWrapper{a: &protocol.ReduceAssignment{}, state: aReady}
	taskD.state = tDone
	taskDead.kill()
	for taskE.assign() != nil {
	}
	tests := []struct {
		name  string
		task  *task
		isNil bool
		isMap bool
	}{
		{"map", taskM, false, true},
		{"reduce", taskR, false, false},
		{"done", taskD, true, false},
		{"empty", taskE, true, false},
		{"dead", taskDead, true, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := test.task.assign()
			if test.isNil {
				if a != nil {
					t.Errorf("task.assign() should return nil")
				}
				return
			}
			if !test.isNil && a == nil {
				t.Errorf("task.assign() should return assignment")
			}
			if _, ok := a.(*protocol.MapAssignment); test.isMap && !ok {
				t.Errorf("task.assign() should return map assignment")
			}
			if _, ok := a.(*protocol.ReduceAssignment); !test.isMap && !ok {
				t.Errorf("task.assign() should return reduce assignment")
			}
		})
	}
}

func TestCancel(t *testing.T) {
	taskM := getTestTask(t, 0)
	taskR := getTestTask(t, 1)
	taskR.state = tReducing
	a := &protocol.ReduceAssignment{TID: taskR.id}
	taskR.rA[a.AssignmentID()] = &aWrapper{a: a, state: aReady}

	tests := []struct {
		name string
		task *task
	}{
		{"map", taskM},
		{"reduce", taskR},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var a protocol.Assignment
			for {
				temp := test.task.assign()
				if temp == nil {
					break
				}
				a = temp
			}
			if a == nil {
				t.Fatal("task.assign() should return assignment")
			}
			test.task.cancel(a)
			if test.task.assign() == nil {
				t.Errorf("cancelled test should be available")
			}
		})
	}
}

func TestFinish(t *testing.T) {
	task := getTestTask(t, 0)
	for range task.nMap {
		a := task.assign()
		if a == nil {
			t.Fatalf("task.assign() should return assignment")
		}
		if _, ok := a.(*protocol.MapAssignment); !ok {
			t.Fatalf("task.assign() should return map assignment")
		}
		outputs := make(map[int]*string)
		name0 := fmt.Sprintf("%vtemp0", a.AssignmentID())
		outputs[0] = &name0
		name1 := fmt.Sprintf("%vtemp1", a.AssignmentID())
		outputs[1] = &name1
		task.finish(a, outputs)
	}
	if task.state != tReducing {
		t.Fatalf("task.state should be tReducing")
	}

	seen := make(map[int]bool, task.nReduce)
	for range task.nReduce {
		a := task.assign()
		if a == nil {
			t.Fatalf("task.assign() should return assignment")
		}
		ra, ok := a.(*protocol.ReduceAssignment)
		if !ok {
			t.Fatalf("task.assign() should return reduce assignment")
		}
		h, err := strconv.Atoi(*ra.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if seen[h] {
			t.Fatalf("reduce assignment %d is repeated", h)
		}
		if h > task.nReduce {
			t.Fatalf("reduce assignment %d is out of range", h)
		}
		seen[h] = true
		outputs := make(map[int]*string)
		nameO := fmt.Sprintf("%v_result_%v", a.TaskID(), h)
		outputs[h] = &nameO
		task.finish(a, outputs)
	}
	if task.state != tDone {
		t.Fatalf("task.state should be tDone")
	}
}

func TestRedoTemp(t *testing.T) {
	task := getTestTask(t, 0)
	for range task.nMap {
		a := task.assign()
		if a == nil {
			t.Fatalf("task.assign() should return assignment")
		}
		ma, ok := a.(*protocol.MapAssignment)
		if !ok {
			t.Fatalf("task.assign() should return map assignment")
		}
		offset := int(ma.Offset)
		outputs := make(map[int]*string)
		name0 := fmt.Sprintf("offset_%v_hash_%v", offset, 0)
		name1 := fmt.Sprintf("offset_%v_hash_%v", offset, 1)
		outputs[0] = &name0
		outputs[1] = &name1
		task.finish(a, outputs)
	}

	a := task.assign()
	if a == nil {
		t.Fatalf("task.assign() should return assignment")
	}
	ra, ok := a.(*protocol.ReduceAssignment)
	if !ok {
		t.Fatalf("task.assign() should return map assignment")
	}
	temp := ra.Inputs[0]
	err := task.redoTemp(a, temp)
	if err != nil {
		t.Fatal(err)
	}
	if task.state != tMapping {
		t.Fatalf("task.state should be tMapping")
	}
	a = task.assign()
	if a == nil {
		t.Fatalf("task.assign() should return assignment")
	}
	ma, ok := a.(*protocol.MapAssignment)
	if !ok {
		t.Fatalf("task.assign() should return map assignment")
	}
	offset, err := strconv.Atoi(strings.Split(*temp, "_")[1])
	if err != nil {
		t.Fatal(err)
	}
	if ma.Offset != int64(offset) {
		t.Fatalf("task.assign() should return map assignment of offset %v", offset)
	}

	outputs := make(map[int]*string)
	name0 := fmt.Sprintf("offset_%v_hash_%v", offset, 0)
	name1 := fmt.Sprintf("offset_%v_hash_%v", offset, 1)
	outputs[0] = &name0
	outputs[1] = &name1
	task.finish(a, outputs)

	seen := make(map[int]bool, task.nReduce)
	for range task.nReduce {
		a = task.assign()
		if a == nil {
			t.Fatalf("task.assign() should return assignment")
		}
		ra, ok := a.(*protocol.ReduceAssignment)
		if !ok {
			t.Fatalf("task.assign() should return reduce assignment")
		}
		h, err := strconv.Atoi(*ra.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if seen[h] {
			t.Fatalf("reduce assignment %d is repeated", h)
		}
		if h > task.nReduce {
			t.Fatalf("reduce assignment %d is out of range", h)
		}
		seen[h] = true
		outputs := make(map[int]*string)
		nameO := fmt.Sprintf("%v_result_%v", a.TaskID(), h)
		outputs[h] = &nameO
		task.finish(a, outputs)
	}
	if task.state != tDone {
		t.Fatalf("task.state should be tDone")
	}
}

func TestIntegratedAM(t *testing.T) {
	tempDir := t.TempDir()
	small := filepath.Join(tempDir, "small")
	large := filepath.Join(tempDir, "file2")
	content := []byte("hello world")
	var smallMSize int64 = 1
	smallNMap := len(content) / int(smallMSize)
	err := os.WriteFile(small, content, 0644)
	content = []byte("hello world and good night")
	var largeMSize int64 = 1
	largeNMap := len(content) / int(largeMSize)
	err = os.WriteFile(large, content, 0644)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		r          protocol.StartTaskRequest
		expNMap    int
		expNReduce int
	}{
		{"small", protocol.StartTaskRequest{&small, smallMSize, 10, nil, nil}, smallNMap, 10},
		{"large", protocol.StartTaskRequest{&large, largeMSize, 20, nil, nil}, largeNMap, 20},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker := "main"
			am := newAssignmentManager()
			id, err := am.addTask(test.r)
			if err != nil {
				t.Fatal(err)
			}
			for range test.expNMap {
				a := am.assignTask(worker)
				if a == nil {
					t.Fatalf("am.assignTask() should return assignment")
				}
				ma, ok := a.(*protocol.MapAssignment)
				if !ok {
					t.Fatalf("missing map assignment")
				}
				outputs := make(map[int]*string)
				for i := 0; i < test.expNReduce; i++ {
					name := fmt.Sprintf("offset_%v_hash_%v", ma.Offset, i)
					outputs[i] = &name
				}
				am.finishAssignment(&worker, outputs)
			}
			d, err := am.isTaskDone(id)
			if err != nil {
				t.Fatal(err)
			}
			if d {
				t.Fatalf("task should not be done")
			}

			for range test.expNReduce {
				a := am.assignTask(worker)
				if a == nil {
					t.Fatalf("am.assignTask() should return assignment")
				}
				ra, ok := a.(*protocol.ReduceAssignment)
				if !ok {
					t.Fatalf("task.assign() should return reduce assignment")
				}
				outputs := make(map[int]*string)
				h, err := strconv.Atoi(*ra.Hash)
				if err != nil {
					t.Fatal(err)
				}
				name := fmt.Sprintf("result_hash_%v", h)
				outputs[h] = &name
				am.finishAssignment(&worker, outputs)
			}
			d, err = am.isTaskDone(id)
			if err != nil {
				t.Fatal(err)
			}
			if !d {
				t.Fatalf("task should be done")
			}
			results, err := am.getResult(id)
			if err != nil {
				t.Fatal(err)
			}
			for h, path := range results {
				if *path != fmt.Sprintf("result_hash_%v", h) {
					t.Fatalf("mismatched result")
				}
			}
		})
	}
}
