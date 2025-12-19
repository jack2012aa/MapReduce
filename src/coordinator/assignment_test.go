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

	tests := []struct {
		name     string
		inputs   []string
		nReduce  int
		plugin   string
		opts     []taskOption
		expMA    int
		expRA    int
		expMSize int64
	}{
		{"Single File", []string{file1}, 10, "pg", []taskOption{}, 1, 0, 32 * 1024 * 1024},
		{"Multiple Files", []string{file1, file2}, 10, "pg", []taskOption{}, 2, 0, 32 * 1024 * 1024},
		{"With", []string{file1}, 10, "pg", []taskOption{withMapSize(mSize)}, nMap, 0, mSize},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task, err := makeTask(test.inputs, test.nReduce, test.plugin, test.opts...)
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
			if task.plugin != test.plugin {
				t.Errorf("got plugin %s, want %s", task.plugin, test.plugin)
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

	task, err := makeTask([]string{file}, 1, "", withMapSize(int64(1)))
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
	taskR.rA["1"] = &aWrapper{a: &protocol.Assignment{}, state: aReady}
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
			if test.isMap && !a.IsMap() {
				t.Errorf("task.assign() should return map assignment")
			}
			if !test.isMap && a.IsMap() {
				t.Errorf("task.assign() should return reduce assignment")
			}
		})
	}
}

func TestCancel(t *testing.T) {
	taskM := getTestTask(t, 0)
	taskR := getTestTask(t, 1)
	taskR.state = tReducing
	a := &protocol.Assignment{TaskID: taskR.id}
	taskR.rA[a.ID()] = &aWrapper{a: a, state: aReady}

	tests := []struct {
		name string
		task *task
	}{
		{"map", taskM},
		{"reduce", taskR},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var a *protocol.Assignment
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
		if !a.IsMap() {
			t.Fatalf("task.assign() should return map assignment")
		}
		outputs := make(map[int]string)
		outputs[0] = a.ID() + "temp0"
		outputs[1] = a.ID() + "temp1"
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
		if a.IsMap() {
			t.Fatalf("task.assign() should return reduce assignment")
		}
		h, err := strconv.Atoi(a.Reduce.Hash)
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
		outputs := make(map[int]string)
		outputs[h] = fmt.Sprintf("%v_result_%v", a.ID(), h)
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
		offset := int(a.Map.Offset)
		outputs := make(map[int]string)
		outputs[0] = fmt.Sprintf("offset_%v_hash_%v", offset, 0)
		outputs[1] = fmt.Sprintf("offset_%v_hash_%v", offset, 1)
		task.finish(a, outputs)
	}

	a := task.assign()
	if a == nil {
		t.Fatalf("task.assign() should return assignment")
	}
	if a.IsMap() {
		t.Fatalf("task.assign() should return reduce assignment")
	}
	temp := a.Reduce.Inputs[0]
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
	if !a.IsMap() {
		t.Fatalf("task.assign() should return map assignment")
	}
	offset, err := strconv.Atoi(strings.Split(temp, "_")[1])
	if err != nil {
		t.Fatal(err)
	}
	if a.Map.Offset != int64(offset) {
		t.Fatalf("task.assign() should return map assignment of offset %v", offset)
	}

	outputs := make(map[int]string)
	outputs[0] = fmt.Sprintf("offset_%v_hash_%v", offset, 0)
	outputs[1] = fmt.Sprintf("offset_%v_hash_%v", offset, 1)
	task.finish(a, outputs)

	seen := make(map[int]bool, task.nReduce)
	for range task.nReduce {
		a = task.assign()
		if a == nil {
			t.Fatalf("task.assign() should return assignment")
		}
		if a.IsMap() {
			t.Fatalf("task.assign() should return reduce assignment")
		}
		h, err := strconv.Atoi(a.Reduce.Hash)
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
		outputs := make(map[int]string)
		outputs[h] = fmt.Sprintf("%v_result_%v", a.ID(), h)
		task.finish(a, outputs)
	}
	if task.state != tDone {
		t.Fatalf("task.state should be tDone")
	}
}
