package sdk

import (
	"MapReduce/src/protocol"
	"errors"
	"net/rpc"
	"time"
)

type MapReduce struct {
	coord  string
	client *rpc.Client
}

func MakeMapReduce(coord string) (*MapReduce, error) {
	client, err := rpc.DialHTTP("tcp", coord)
	if err != nil {
		return nil, err
	}
	return &MapReduce{coord: coord, client: client}, nil
}

func (m *MapReduce) isTaskDone(id *protocol.ID) (bool, error) {
	req := protocol.AskTaskRequest{TaskID: id}
	resp := protocol.AskTaskResponse{}
	err := m.client.Call("Coordinator.AskTask", req, &resp)
	if err != nil {
		return false, err
	}
	if resp.State == protocol.TaskDead {
		return false, errors.New("task is dead")
	}
	return resp.State == protocol.TaskDone, nil
}

func (m *MapReduce) getResult(id *protocol.ID) (map[int]*string, error) {
	req := protocol.GetResultRequest{TaskID: id}
	resp := protocol.GetResultResponse{}
	err := m.client.Call("Coordinator.GetResult", req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Output, nil
}

func (m *MapReduce) startTask(input *string, mapSize int64, nReduce int, plugin *string) (map[int]*string, error) {
	req := protocol.StartTaskRequest{Input: input, MapSize: mapSize, NReduce: nReduce, Plugin: plugin}
	resp := protocol.StartTaskResponse{}
	err := m.client.Call("Coordinator.StartTask", req, &resp)
	if err != nil {
		return nil, err
	}
	id := resp.TaskID
	for {
		d, err := m.isTaskDone(id)
		if err != nil {
			return nil, err
		}
		if !d {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	outputs, err := m.getResult(id)
	if err != nil {
		return nil, err
	}
	return outputs, nil
}
