package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type (
	MapF    func(string, string) []KeyValue
	ReduceF func(string, []string) string
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf MapF, reducef ReduceF) {
	for {
		task, nReduce, err := fetchTask()
		if err != nil {
			log.Fatalf("cannot fetch any task.")
		}
		switch task.Type {
		case MapTask:
			err := doMap(mapf, task, nReduce)
			if err != nil {
				log.Fatalf("connot process Map")
			}
			reportTask(task)
		case ReduceTask:
			err := doReduce(reducef, task)
			if err != nil {
				log.Fatalf("connot procee Reduce")
			}
			reportTask(task)
		case ExitTask:
			return
		}
	}
}

func doMap(mapf MapF, task *Task, nReduce int) error {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
		return err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
		return err
	}
	kva := mapf(task.File, string(content))
	_ = kva

	files := make([]*os.File, nReduce)
	buffers := make([]*bufio.Writer, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", task.Index, i)
		files[i], err = os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		buffers[i] = bufio.NewWriter(files[i])
		encoders[i] = json.NewEncoder(buffers[i])
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		encoders[idx].Encode(&kv)
	}

	for i := 0; i < nReduce; i++ {
		err := buffers[i].Flush()
		if err != nil {
			log.Fatalf("cannot flush buffer %v", files[i].Name())
		}
		files[i].Close()
	}

	return nil
}

func doReduce(reducef ReduceF, task *Task) error {
	files, err := filepath.Glob(fmt.Sprintf("mr-%v-%v", "*", task.Index))
	if err != nil {
		log.Fatalf("connot obtain reduce files")
	}

	var kv KeyValue
	kvMap := make(map[string][]string)

	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("connot open file %v", filePath)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		for decoder.More() {
			decoder.Decode(&kv)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	keys := []string{}
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outputFileName := fmt.Sprintf("mr-out-%v", task.Index)
	file, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("connot create file %v", outputFileName)
	}

	for _, key := range keys {
		output := reducef(key, kvMap[key])
		fmt.Fprintf(file, "%v %v\n", key, output)
	}
	file.Close()

	return nil
}

func fetchTask() (*Task, int, error) {
	args := &FetchTaskArgs{Worker: os.Getpid()}
	reply := &FetchTaskReply{}
	err := call("Coordinator.FetchTask", args, reply)
	return reply.Task, reply.NReduce, err
}

func reportTask(task *Task) error {
	args := &ReportTaskArgs{Task: task}
	reply := &ReportTaskReply{}
	err := call("Coordinator.ReportTask", args, reply)
	return err
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
