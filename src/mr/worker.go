package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerId int
var nReduce int

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
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
	log.Printf("mapping to %d reduce tasks\n", reply.NReduce)
    nReduce = reply.NReduce

    for {
        task, err := CallGetTask()
        if err != nil {
            log.Printf("get task error: %v\n", err)
        } else {
            switch task.Type {
            case MapTask:
                log.Printf("got map task %d\n", task.Id)
                err = ExecuteMap(task.InputPaths[0], task.Id, mapf)
            case ReduceTask:
                log.Printf("got reduce task %d\n", task.Id)
                err = ExecuteReduce(task.InputPaths, task.Id, reducef)
            }
            if err != nil {
                log.Printf("execute task error: %v\n", err)
            } else {
                err = CallCompleteTask(task.Id)
            }
        }
    }
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

func ExecuteMap(filename string, taskId int, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))

	// create intermediate files and encoders
	outFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFiles[i], err = os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			return err
		}
		encs[i] = json.NewEncoder(outFiles[i])
	}

	// write to intermediate files
	for _, kv := range kva {
        hash := ihash(kv.Key)
		err := encs[hash%nReduce].Encode(&kv)
		if err != nil {
			return err
		}
	}

	// close all the files
	for i := 0; i < nReduce; i++ {
		outFiles[i].Close()
	}

	// atomically rename temp files
	for i := 0; i < nReduce; i++ {
		err := os.Rename(outFiles[i].Name(), intermediateName(taskId, i))
		if err != nil {
			return err
		}
	}

	return nil
}

func ExecuteReduce(filenames []string, taskId int,
	reducef func(string, []string) string) error {

	kva := make([]KeyValue, 0)

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort kva by key
	sort.Sort(ByKey(kva))

	// create temp file
	tmpFile, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tmpFile.Close()

	// rename temp file
	return os.Rename(tmpFile.Name(), reduceName(taskId))
}

func CallCompleteTask(taskId int) error {
	args := &CompleteTaskArgs{
        TaskId: taskId,
    }
	reply := &struct{}{}
	return call("Coordinator.CompleteTask", args, reply)
}

// call the coordinator to get the next task

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
