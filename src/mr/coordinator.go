package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Map = iota
	Reduce
	Sleep
)

const (
	Working = iota
	Timeout
)

const (
	NotStarted = iota
	Processing
	Finished
)

type Task struct {
	Name      string
	Type      int
	Status    int //任务状态正常或超时
	mFileName string
	rFileName int
}

var taskNumber int = 0

type Coordinator struct {
	// Your definitions here.
	mRecord      map[string]int
	rRecord      map[int]int
	reduceFile   map[int][]string
	taskMap      map[string]*Task
	mCount       int //记录已经完成的map的任务数量
	rCount       int //记录已经完成的reduce的任务数量，这个地方可以用rCount == reduceNumber来判断是否已经完成所有任务，或者再额外加一个状态变量用来记录
	mapFinished  bool
	reduceNumber int
	mutex        sync.Mutex
}

func (c *Coordinator) HandleTimeOut(taskName string) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if t, ok := c.taskMap[taskName]; ok {
		t.Status = Timeout
		if t.Type == Map {
			f := t.mFileName
			if c.mRecord[f] == Processing {
				c.mRecord[f] = NotStarted
			}
		} else if t.Type == Reduce {
			f := t.rFileName
			if c.rRecord[f] == Processing {
				c.rRecord[f] = NotStarted
			}
		}
	}
}

func (c *Coordinator) Report(args *ReportStatusRequest, reply *ReportStatusResponse) error {
	reply.X = 1
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if t, ok := c.taskMap[args.TaskName]; ok {
		flag := t.Status
		if flag == Timeout {
			delete(c.taskMap, args.TaskName)
			return nil
		}

		tType := t.Type
		if tType == Map {
			f := t.mFileName
			c.mRecord[f] = Finished
			c.mCount += 1
			if c.mCount == len(c.mRecord) {
				c.mapFinished = true
			}
			for _, v := range args.FileName {
				index := strings.LastIndex(v, "-")
				num, err := strconv.Atoi(v[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				c.reduceFile[num] = append(c.reduceFile[num], v)
			}
			delete(c.taskMap, t.Name)
			return nil
		} else if tType == Reduce {
			rf := t.rFileName
			c.rRecord[rf] = Finished
			c.rCount += 1
			delete(c.taskMap, t.Name)
			return nil
		} else {
			log.Fatal("task type is not map and reduce")
		}
	}
	log.Println("%s task is not in Master record", args.TaskName)
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.RFileName = make([]string, 0)
	reply.ReduceNumber = c.reduceNumber
	reply.MFileName = ""
	reply.TaskName = strconv.Itoa(taskNumber)
	taskNumber += 1
	if c.mapFinished {
		for v := range c.rRecord {
			flag := c.rRecord[v]
			if flag == Processing || flag == Finished {
				continue
			} else {
				c.rRecord[v] = Processing
				for _, fillName := range c.reduceFile[v] {
					reply.RFileName = append(reply.RFileName, fillName)
				}
				reply.TaskType = Reduce
				t := &Task{reply.TaskName, reply.TaskType, Working, "", v}
				c.taskMap[reply.TaskName] = t

				//cursh处理
				go c.HandleTimeOut(reply.TaskName)

				return nil
			}
		}
		reply.TaskType = Sleep
		return nil
	} else {
		for v, _ := range c.mRecord {
			flag := c.mRecord[v]
			if flag == Processing || flag == Finished {
				continue
			} else {
				c.mRecord[v] = Processing
				reply.MFileName = v
				reply.TaskType = Map
				t := &Task{reply.TaskName, reply.TaskType, Working, reply.MFileName, -1}
				c.taskMap[reply.TaskName] = t

				go c.HandleTimeOut(reply.TaskName)
				return nil
			}
		}
		reply.TaskType = Sleep
		return nil
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.rCount == c.reduceNumber {
		ret = true
	}
	// Your code here.
	// time.Sleep(200 * time.Millisecond)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//创建一个master实例
	c := Coordinator{
		mRecord:      make(map[string]int),   //记录需要map的文件，0表示未执行，1表示正在执行，2表示已完成
		rRecord:      make(map[int]int),      //记录需要reduce的文件，0表示未执行，1表示正在执行，2表示已完成
		reduceFile:   make(map[int][]string), //记录中间文件
		taskMap:      make(map[string]*Task), //任务池
		mCount:       0,                      //记录已经完成的map的任务数量
		rCount:       0,                      //记录已经完成的reduce的任务数量
		mapFinished:  false,
		reduceNumber: nReduce,
		mutex:        sync.Mutex{},
	}

	// Your code here.
	//标记需要处理的文件
	log.Println("MakeMaster")
	for _, f := range files {
		c.mRecord[f] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.rRecord[i] = 0
	}

	//启动master的server，server中先利用rpc注册了master实例的服务，然后监听服务，开启http.Server进程
	c.server()
	return &c
}
