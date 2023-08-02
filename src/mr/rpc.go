package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type GetTaskRequest struct {
	X int
}

type GetTaskResponse struct {
	MFileName string
	TaskName  string
	RFileName []string
	TaskType int //任务类别, 0:map, 1:reduce, 2:sleep
	ReduceNumber int //需要将中间文件分组的数量
}

//worker上报任务完成状态
type ReportStatusRequest struct {
	FileName []string //map任务告知master节点中间文件的信息
	TaskName string //该任务的名字
}

type ReportStatusResponse struct {
	X int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
