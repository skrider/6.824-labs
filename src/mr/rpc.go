package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
	NReduce  int
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Type       TaskType
	InputPaths []string // for map tasks
	Id         int
}

type CompleteTaskArgs struct {
	TaskId int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
