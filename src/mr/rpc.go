package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// 任务类型常量
const (
	MapTask    = 0
	ReduceTask = 1
	WaitTask   = 2
	EndTask    = 3
)

type TaskInfo struct {
	// 任务状态
	TaskType int
	// 文件名称
	FileName string
	// 文件索引
	FileIndex int
	// 分区索引
	PartIndex int

	NReduce int
	NFiles  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
