package protocol

/**SDK <-> Coordinator*/

type StartTaskRequest struct {
	Input   *string
	MapSize int64
	NReduce int
	Plugin  *string
}

type StartTaskResponse struct {
	TaskID *ID
}

type GetResultRequest struct {
	TaskID *ID
}

type GetResultResponse struct {
	Output map[int]*string
}

type AskTaskRequest struct {
	TaskID *ID
}

type AskTaskResponse struct {
	State string
}

// Task states
const (
	TaskDead    = "dead"
	TaskDone    = "done"
	TaskWorking = "working"
)

type RegisterWorkerRequest struct {
	Addr *string
}

type RegisterWorkerResponse struct{}

/** Worker <-> Coordinator */

type HeartBeatRequest struct {
	Source *string
}

type HeartBeatResponse struct{}

type AskAssignmentRequest struct {
	Source *string
}

type AskAssignmentResponse struct {
	A       Assignment
	IsEmpty bool
}

type FinishAssignmentRequest struct {
	Outputs map[int]*string
	Source  *string
}

type FinishAssignmentResponse struct{}

type DeadTempRequest struct {
	Source *string
	Temp   *string
}

type DeadTempResponse struct{}

type ReportErrorRequest struct {
	Source *string
	Error  error
}

type ReportErrorResponse struct{}
