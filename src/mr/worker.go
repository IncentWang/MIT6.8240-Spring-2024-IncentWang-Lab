package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// Work as Mapper
	// if all mapper work is done, then turn to work as reducer

	// Work Progress means worker status: 0 means gonna work as mapper, 1 means gonna work as reducer, 2 means done work
	workProgress := 0

	for workProgress != 2 {
		if workProgress == 0 {
			emptyArgs := EmptyMessage{}
			reply1 := GetMapTaskReply{}
			ok := call("Coordinator.GetFirstAvailiableMapTask", &emptyArgs, &reply1)
			if ok {
				// Should do map work here use mapf

				// First ask coordinator to assign this map task to itself, if assign successfully, continue, else, wait to next attempt
				// When TaskNumber == -1, means there are no more map tasks to do with
				if reply1.TaskNumber == -1 {
					workProgress++
					continue
				}

				// When TaskNumer == -2, means there are no "availiable map tasks to do with"
				if reply1.TaskNumber == -2 {
					time.Sleep(3 * time.Second)
					continue
				}

				assignTaskArgs := SingleIndexArg{}
				assignTaskArgs.Index = reply1.TaskNumber
				assignMapTaskReply := StartMapTaskReply{}
				ok = call("Coordinator.AssignMapTask", &assignTaskArgs, &assignMapTaskReply)
				if !ok {
					time.Sleep(3 * time.Second)
					continue
				}

				// Read contents as string to mapContent variable
				file, err := os.Open(reply1.FileName)
				if err != nil {
					log.Fatalf("Fatal Error happened when opening %v", reply1.FileName)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Fatal Error happened when reading content %v", reply1.FileName)
				}
				mapResult := mapf(reply1.FileName, string(content))

				// For each key value pair map function generated, split it to different temp files with name mr-#MAPTASK-#REDUCETASK
				allTempFiles := make([]os.File, reply1.TotalReduceTaskNumber)
				// Create temporary files in directory
				for i := 0; i < reply1.TotalReduceTaskNumber; i++ {
					tFile, err := os.CreateTemp("", "temp*")
					if err != nil {
						log.Fatalf("Fatal Error happened when creating temp files for #%v map task, #%v reduce task number", reply1.TaskNumber, i)
					}
					allTempFiles[i] = *tFile
				}
				for _, ele := range mapResult {
					_, err = allTempFiles[ihash(ele.Key)%reply1.TotalReduceTaskNumber].WriteString(fmt.Sprintf("%v %v\n", ele.Key, ele.Value))
					if err != nil {
						log.Fatalf("Fatal Error happened when writing to temp files with Key: %v, Value: %v", ele.Key, ele.Value)
					}
				}
				for ind, file := range allTempFiles {
					err = os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v.txt", reply1.TaskNumber, ind))
					if err != nil {
						log.Fatalf("Fatal Error happened when renaming temp file to correct file name, #Map: %v, #Reduce: %v", reply1.TaskNumber, ind)
					}
				}
				for _, file := range allTempFiles {
					file.Close()
				}
				file.Close()

				// Done the map task, call Coordinator's DoneMapTask function to realease lock

				doneMapTaskArgs := SingleIndexArg{}
				doneMapTaskArgs.Index = reply1.TaskNumber
				doneMapTaskReply := EmptyMessage{}
				ok = call("Coordinator.DoneMapTask", &doneMapTaskArgs, &doneMapTaskReply)
				if !ok {
					fmt.Println("RPC called failed when sending DoneMapTask function call")
					time.Sleep(3 * time.Second)
					continue
				}
			} else {
				fmt.Println("RPC called failed, code 0000")
			}
		}
		if workProgress == 1 {
			emptyArgs := EmptyMessage{}
			reply2 := GetReduceTaskReply{}
			ok := call("Coordinator.GetFirstAvailiableReduceTask", &emptyArgs, &reply2)
			if ok {
				// Should do reduce work here use reducef

				// When tasknumber == -1, means no more reduce tasks to do with
				if reply2.TaskNumber == -1 {
					workProgress++
					continue
				}

				// When TaskNumber == -2, means no "availiable reduce tasks to assign"
				if reply2.TaskNumber == -2 {
					time.Sleep(3 * time.Second)
					continue
				}

				// Should ask Coordinator to assign the reduce task to itself
				assignReduceTaskArgs := SingleIndexArg{}
				assignReduceTaskArgs.Index = reply2.TaskNumber
				assignReduceTaskReply := StartReduceTaskReply{}
				ok = call("Coordinator.AssignReduceTask", &assignReduceTaskArgs, &assignReduceTaskReply)
				if !ok {
					time.Sleep(3 * time.Second)
					continue
				}

				// Gather info from all temportary files: mr-#MapTask-reply2.TaskNumber.txt
				// Combine it to the same map, then write the map to mr-out-TaskNumber.txt
				reduceMap := make(map[string][]string)
				for i := 0; i < reply2.TotalMapTaskNumber; i++ {
					fileName := fmt.Sprintf("mr-%v-%v.txt", i, reply2.TaskNumber)
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("Fatal Error happened when opening temporary file: mr-%v-%v.txt", i, reply2.TaskNumber)
					}
					scanner := bufio.NewScanner(file)
					for scanner.Scan() {
						res := strings.Split(scanner.Text(), " ")
						val, ok := reduceMap[res[0]]
						if !ok {
							val = make([]string, 0)
							reduceMap[res[0]] = val
						}
						reduceMap[res[0]] = append(reduceMap[res[0]], res[1])
					}
					file.Close()
				}
				// Analysis reduceMap to produce final result
				file, err := os.Create(fmt.Sprintf("mr-out-%v", reply2.TaskNumber))
				if err != nil {
					log.Fatalf("Fatal Error happened when opening output file: mr-out-%v", reply2.TaskNumber)
				}
				for k, v := range reduceMap {
					output := reducef(k, v)
					fmt.Fprintf(file, "%v %v\n", k, output)
				}
				file.Close()
				doneReduceTaskArgs := SingleIndexArg{}
				doneReduceTaskReply := EmptyMessage{}
				doneReduceTaskArgs.Index = reply2.TaskNumber
				ok = call("Coordinator.DoneReduceTask", &doneReduceTaskArgs, &doneReduceTaskReply)
				if !ok {
					fmt.Println("RPC called failed when sending DoneReduceTask function call")
					time.Sleep(3 * time.Second)
					continue
				}
			} else {
				fmt.Println("RPC called failed, code 0001")
			}
		}
		time.Sleep(3) // Sleep for 3 seconds till next attempt for a new task
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
