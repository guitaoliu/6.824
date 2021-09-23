package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type (
	TaskType   int
	TaskStatus int
)

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	NoTask
)

const (
	Wait TaskStatus = iota
	Processing
	Done
)

type Task struct {
	Type   TaskType
	File   string
	Status TaskStatus
	Index  int
	Worker int
}

type Coordinator struct {
	nReduce     int
	nMap        int
	reduceTasks []*Task
	mapTasks    []*Task
	mu          sync.RWMutex
}

func (c *Coordinator) FetchTask(arg *FetchTaskArgs, reply *FetchTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker := arg.Worker
	var task *Task
	if c.nMap > 0 {
		task = c.pullTask(c.mapTasks, worker)
	} else if c.nReduce > 0 {
		task = c.pullTask(c.reduceTasks, worker)
	} else {
		task = &Task{Type: ExitTask}
	}
	reply.Task = task
	reply.NReduce = len(c.reduceTasks)
	return nil
}

func (c *Coordinator) pullTask(tasks []*Task, worker int) *Task {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].Status == Wait {
			tasks[i].Status = Processing
			tasks[i].Worker = worker
			return tasks[i]
		}
	}
	return &Task{Type: NoTask}
}

func (c *Coordinator) ReportTask(arg *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	switch arg.Task.Type {
	case MapTask:
		task = c.mapTasks[arg.Task.Index]
		if task.Worker == arg.Task.Worker && task.Status == Processing {
			task.Status = Done
			c.nMap--
		}
	case ReduceTask:
		task = c.reduceTasks[arg.Task.Index]
		if task.Worker == arg.Task.Worker && task.Status == Processing {
			task.Status = Done
			c.nReduce--
		}
	default:
		return nil
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.nMap == 0 && c.nReduce == 0 {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("nMap: %v", len(files))
	log.Printf("nReduce: %v", nReduce)
	c := Coordinator{
		nReduce:     nReduce,
		nMap:        len(files),
		reduceTasks: make([]*Task, 0, len(files)),
		mapTasks:    make([]*Task, 0, nReduce),
	}

	for i := 0; i < c.nMap; i++ {
		task := &Task{
			Type:   MapTask,
			File:   files[i],
			Status: Wait,
			Index:  i,
			Worker: -1,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < c.nReduce; i++ {
		task := &Task{
			Type:   ReduceTask,
			Status: Wait,
			Index:  i,
			Worker: -1,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	c.server()
	return &c
}
