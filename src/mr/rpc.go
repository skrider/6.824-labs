package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

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

func intermediateName(mapTask int, reduceTask int) string {
	return fmt.Sprintf("mr-temp-%d-%d", mapTask, reduceTask)
}

func reduceName(reduceTask int) string {
	return fmt.Sprintf("mr-out-%d", reduceTask)
}
