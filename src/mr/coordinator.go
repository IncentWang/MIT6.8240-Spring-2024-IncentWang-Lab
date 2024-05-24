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
	// Your definitions here.
	mapTasks        []int
	reduceTasks     []int
	files           []string
	mapTaskLocks    []sync.Mutex
	reduceTaskLocks []sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	ret = c.CheckAllMapTaskDone() && c.CheckAllReduceTaskDone()

	return ret
}

// Return value: (number of current task, should use which file, total reduce tasks amount)
func (c *Coordinator) GetFirstAvailiableMapTask(args *EmptyMessage, reply *GetMapTaskReply) error {
	if c.CheckAllMapTaskDone() {
		reply.TaskNumber = -1
		reply.FileName = ""
		reply.TotalReduceTaskNumber = len(c.reduceTasks)

		return nil
	}
	for ind, ele := range c.mapTasks {
		if ele == 0 {
			reply.TaskNumber = ind
			reply.FileName = c.files[ind]
			reply.TotalReduceTaskNumber = len(c.reduceTasks)
			return nil
		}
	}
	reply.TaskNumber = -2
	reply.FileName = ""
	reply.TotalReduceTaskNumber = len(c.reduceTasks)
	return nil
}

// Return value: (number of current reduce task, total number of map tasks)
func (c *Coordinator) GetFirstAvailiableReduceTask(args *EmptyMessage, reply *GetReduceTaskReply) error {
	if c.CheckAllReduceTaskDone() {
		reply.TaskNumber = -1
		reply.TotalMapTaskNumber = len(c.mapTasks)
		return nil
	}
	for ind, ele := range c.reduceTasks {
		if ele == 0 {
			reply.TaskNumber = ind
			reply.TotalMapTaskNumber = len(c.mapTasks)
			reply.Metadata = ""
			return nil
		}
	}
	reply.TaskNumber = -2
	reply.TotalMapTaskNumber = len(c.mapTasks)
	return nil
}

func (c *Coordinator) AssignMapTask(args *SingleIndexArg, reply *StartMapTaskReply) error {
	// Assign the indexed map task to this worker.
	// If the mapTasks[taskIndex] is not 0, race condition, return (-1, "") for worker re-attempt
	// If the mapTasks[taskIndex] is 0, change it to 1
	// After assign tasks, start a timed task to check mapTasks[taskIndex] in 10 seconds. if it is still 1, means worker died, change it back to 0
	taskIndex := args.Index
	c.mapTaskLocks[taskIndex].Lock()
	defer c.mapTaskLocks[taskIndex].Unlock()

	if c.mapTasks[taskIndex] != 0 {
		reply.Ok = false
		reply.FileName = ""
		return nil
	}
	c.mapTasks[taskIndex] = 1
	// Timed tasks here to change back status when worker dies here
	go c.MapTaskWatcher(taskIndex)
	reply.Ok = true
	reply.FileName = c.files[taskIndex]
	return nil
}

func (c *Coordinator) AssignReduceTask(args *SingleIndexArg, reply *StartReduceTaskReply) error {
	taskIndex := args.Index
	c.reduceTaskLocks[taskIndex].Lock()
	defer c.reduceTaskLocks[taskIndex].Unlock()

	if c.reduceTasks[taskIndex] != 0 {
		reply.Ok = false
		return nil
	}

	c.reduceTasks[taskIndex] = 1
	go c.ReduceTaskWatcher(taskIndex)
	reply.Ok = true
	return nil
}

func (c *Coordinator) MapTaskWatcher(taskIndex int) {
	time.Sleep(10000 * time.Millisecond)
	c.mapTaskLocks[taskIndex].Lock()
	defer c.mapTaskLocks[taskIndex].Unlock()
	if c.mapTasks[taskIndex] != 2 {
		c.mapTasks[taskIndex] = 0
	}
}

func (c *Coordinator) ReduceTaskWatcher(taskIndex int) {
	time.Sleep(10000 * time.Millisecond)
	c.reduceTaskLocks[taskIndex].Lock()
	defer c.reduceTaskLocks[taskIndex].Unlock()
	if c.reduceTasks[taskIndex] != 2 {
		c.reduceTasks[taskIndex] = 0
	}
}

func (c *Coordinator) DoneMapTask(args *SingleIndexArg, reply *EmptyMessage) error {
	taskIndex := args.Index
	c.mapTaskLocks[taskIndex].Lock()
	defer c.mapTaskLocks[taskIndex].Unlock()
	c.mapTasks[taskIndex] = 2
	return nil
}

func (c *Coordinator) DoneReduceTask(args *SingleIndexArg, reply *EmptyMessage) error {
	taskIndex := args.Index
	c.reduceTaskLocks[taskIndex].Lock()
	defer c.reduceTaskLocks[taskIndex].Unlock()
	c.reduceTasks[taskIndex] = 2
	return nil
}

func (c *Coordinator) CheckAllMapTaskDone() bool {
	for _, ele := range c.mapTasks {
		if ele != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) CheckAllReduceTaskDone() bool {
	for _, ele := range c.reduceTasks {
		if ele != 2 {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// Maintain an array of int with length is len(files) to tell map task status: 0 non touched, 1 processing, 2 done
	// Maintain an array of int with length is len(nReduce) to tell reduce task status: 0 non touched, 1 processing, 2 done
	// because coordinatior is running under just one thread, no need to add sync logic
	c.mapTasks = make([]int, len(files))
	c.reduceTasks = make([]int, nReduce)
	c.mapTaskLocks = make([]sync.Mutex, len(files))
	c.reduceTaskLocks = make([]sync.Mutex, nReduce)
	c.files = files

	c.server()
	return &c
}
