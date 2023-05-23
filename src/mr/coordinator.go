package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	files      []string
	nReduce    int
	workers    int
	mu         sync.Mutex
	nextTask   chan *task
	mapDone    chan bool
	reduceDone chan bool
	done       bool
	tasks      []*task
}

type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskInProgress
	TaskComplete
	TaskError
)

type task struct {
	taskType   TaskType
	status     TaskStatus
	c          *Coordinator
	inputPaths []string
	id         int
	mu         sync.Mutex
}

func (t *task) Watch() {
	time.Sleep(10 * time.Second)
	t.mu.Lock()
	if t.status == TaskInProgress {
		t.status = TaskError
	}
	t.mu.Unlock()
	t.c.nextTask <- t
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workers += 1
	reply.WorkerId = c.workers
	reply.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-c.nextTask
	task.mu.Lock()
	defer task.mu.Unlock()

	task.status = TaskInProgress
	go task.Watch()

	reply.Type = task.taskType
	reply.Id = task.id
	reply.InputPaths = task.inputPaths

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *struct{}) error {
	id := args.TaskId
	task := c.tasks[id]
	task.mu.Lock()
	defer task.mu.Unlock()
	task.status = TaskComplete
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
    c.mu.Lock()
    defer c.mu.Unlock()

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.workers = 0

	reduceTasks := make([]*task, nReduce)

	for i := 0; i < nReduce; i++ {
		task := &task{
			taskType:   ReduceTask,
			status:     TaskPending,
			inputPaths: []string{},
			id:         len(files) + i,
			c:          &c,
		}
		reduceTasks[i] = task
	}

	mapTasks := make([]*task, len(files))

	for i, file := range files {
		task := &task{
			taskType:   MapTask,
			status:     TaskPending,
			inputPaths: []string{file},
			id:         i,
			c:          &c,
		}
		mapTasks[i] = task
		for j, task := range reduceTasks {
			task.inputPaths = append(task.inputPaths, intermediateName(i, j))
		}
	}

	c.nextTask = make(chan *task)
	c.tasks = append(mapTasks, reduceTasks...)

	c.mapDone = make(chan bool)
	c.reduceDone = make(chan bool)

	go monitorTasks(mapTasks, c.mapDone)
	go monitorTasks(reduceTasks, c.reduceDone)

	go func() {
		for _, task := range mapTasks {
			c.nextTask <- task
		}

		<-c.mapDone

		for _, task := range reduceTasks {
			c.nextTask <- task
		}

		<-c.reduceDone

        c.mu.Lock()
        defer c.mu.Unlock()
        c.done = true

		log.Println("All tasks complete")
	}()

	c.server()
	return &c
}

func monitorTasks(tasks []*task, done chan bool) {
	for {
		d := true
		for _, task := range tasks {
			task.mu.Lock()
			if task.status != TaskComplete {
				d = false
			}
			task.mu.Unlock()
		}
		if d {
			done <- true
		}
	}
}
