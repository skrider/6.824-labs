package mr

import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var workerId int
var nReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
    reply, err := CallRegister()
    workerId = reply.WorkerId
    if err != nil {
        log.Fatal("register error:", err)
    }
    log.Printf("worker %d registered\n", reply.WorkerId)
    log.Printf("mapping to %d map tasks\n", reply.NReduce)

    task, err := CallGetTask()
    if err != nil {
        log.Fatal("get task error:", err)
    }
    log.Printf("got task %d\n", task.Id)
}

// register the worker with the coordinator
func CallRegister() (*RegisterReply, error) {
    args := &RegisterArgs{}
    reply := &RegisterReply{}
    err := call("Coordinator.Register", args, reply)
    if err != nil {
        return reply, err
    }
    return reply, nil
}

func CallGetTask() (*GetTaskReply, error) {
    args := &GetTaskArgs{
        WorkerId: workerId,
    }
    reply := &GetTaskReply{}
    err := call("Coordinator.GetTask", args, reply)
    if err != nil {
        return reply, err
    }
    return reply, nil
}

// call the coordinator to get the next task

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	return err
}
