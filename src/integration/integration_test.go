package integration

import (
	"MapReduce/src/coordinator"
	"MapReduce/src/protocol"
	"MapReduce/src/worker"
	"io"
	"os"
	"path"
	"testing"
)

func TestCoordWorker(t *testing.T) {
	tempDir := os.TempDir()
	m := Map
	r := Reduce
	p := path.Join(tempDir, "test.txt")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	content := "Hello World! I am Chang-Yu, nice to meet you."
	_, err = f.Write([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	tests := []struct {
		Name   string
		Plugin string
		WOpts  []worker.WorkerDaemonOption
		want   map[int]string
	}{
		{
			"load",
			"wc.so",
			[]worker.WorkerDaemonOption{worker.WithBaseDir(tempDir)},
			map[int]string{1: "Hello 1\nWorl 1\nYu 1\nam 1\nang 1\nce 1\nd 1\n", 0: "Ch 1\nI 1\nmeet 1\nni 1\nto 1\nyou 1\n"},
		},
		{
			"pass",
			"",
			[]worker.WorkerDaemonOption{
				worker.WithBaseDir(tempDir),
				worker.WithMapF(m),
				worker.WithReduceF(r),
			},
			map[int]string{1: "Hello 1\nWorl 1\nYu 1\nam 1\nang 1\nce 1\nd 1\n", 0: "Ch 1\nI 1\nmeet 1\nni 1\nto 1\nyou 1\n"},
		},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			c, coordAddr := coordinator.MakeCoordinatorDaemon()
			defer c.Close()
			wd := worker.MakeWorkerDaemon(coordAddr, test.WOpts...)
			defer wd.Close()
			go wd.Start()

			tq := protocol.StartTaskRequest{Input: &p, MapSize: 10, NReduce: 2, Plugin: &test.Plugin}
			tp := protocol.StartTaskResponse{}
			err = c.StartTask(tq, &tp)
			if err != nil {
				t.Fatal(err)
			}
			id := tp.TaskID

			//start := time.Now()
			//deadline := start.Add(10 * time.Second)
			for {
				aq := protocol.AskTaskRequest{TaskID: id}
				ap := protocol.AskTaskResponse{}
				err = c.AskTask(aq, &ap)
				if err != nil || ap.State == protocol.TaskDead {
					t.Fatal(err)
				}
				//if time.Now().After(deadline) {
				//	t.Fatalf("task deadline exceeded")
				//}
				if ap.State == protocol.TaskDone {
					break
				}
			}
			gq := protocol.GetResultRequest{TaskID: id}
			gp := protocol.GetResultResponse{}
			err = c.GetResult(gq, &gp)
			if err != nil {
				t.Fatalf("should get result, but %v", err)
			}
			for h, p := range gp.Output {
				f, err := os.Open(*p)
				if err != nil {
					t.Fatalf("should open output, but %v", err)
				}
				info, err := f.Stat()
				if err != nil {
					t.Fatalf("should stat output, but %v", err)
				}
				l := info.Size()
				buf := make([]byte, l)
				_, err = io.ReadFull(f, buf)
				if err != nil {
					t.Fatalf("should read output, but %v", err)
				}
				f.Close()
				if string(buf) != test.want[h] {
					t.Errorf("got %v\nwant %v", string(buf), test.want[h])
				}
			}
		})
	}

}
