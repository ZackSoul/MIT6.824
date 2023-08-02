package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"encoding/json"
)
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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func HandleReduce(reducef func(string, []string) string, fileNames []string) string {
	files := make([] * os.File, len(fileNames))
	intermediate := []KeyValue{}
	for i := 0; i < len(fileNames); i++ {
		files[i], _ = os.Open(fileNames[i])
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	// log.Println("intermediate", len(intermediate))
	oName := "mr-out-"
	index := fileNames[0][strings.LastIndex(fileNames[0], "-") + 1: ]
	oName = oName + index
	oFile, _ := os.Create(oName)
	i := 0
	//将同一个键的数据合并并交由用户设置的reduce函数处理，并将结果写入到对应文件中
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return oName
}


func HandleMap(mapf func(string, string) []KeyValue, fileName string, fileNum int, taskNum string) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	// log.Println(content)
	if err != nil {
		log.Fatal("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	fileNames := make([]string, fileNum)
	files := make([] * os.File, fileNum)

	for i := 0; i < fileNum; i++ {
		oName := "mr"
		oName = oName + "-" + taskNum + "-" + strconv.Itoa(i)
		// log.Println("create ", oName)
		oFile, _ := os.Create(oName)
		files[i] = oFile
		fileNames[i] = oName
	}
	for _,kv := range intermediate {
		index := ihash(kv.Key) % fileNum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}
	return fileNames
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskRequest{}
		args.X = 0
		reply := GetTaskResponse{}
		call("Coordinator.GetTask", &args, &reply)
		// log.Println("name", reply.TaskName, "type", reply.TaskType)
		if reply.TaskType == Map {
			fileNames := HandleMap(mapf, reply.MFileName, reply.ReduceNumber, reply.TaskName)
			reportArgs := ReportStatusRequest{}
			reportArgs.TaskName = reply.TaskName
			reportArgs.FileName = fileNames
			reportReply := ReportStatusResponse{}
			reportReply.X = 0
			call("Coordinator.Report", &reportArgs, &reportReply)
		} else if reply.TaskType == Reduce {
			HandleReduce(reducef, reply.RFileName)
			reportArgs := ReportStatusRequest{}
			reportArgs.TaskName = reply.TaskName
			reportArgs.FileName = make([]string, 0)
			reportReply := ReportStatusResponse{}
			reportReply.X = 0
			call("Coordinator.Report", &reportArgs, &reportReply)
		} else if reply.TaskType == Sleep {
			time.Sleep(time.Millisecond * 10)
			// log.Println("Sleep Task")
		} else {
			log.Fatal("get task is not map sleep and reduce")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
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
		fmt.Printf("test--------call failed!\n")
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

 
