package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStat struct {
	beginTime time.Time
	fileName  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type MapTaskStat struct {
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		TaskType:  MapTask,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		TaskType:  ReduceTask,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*60)
}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}
func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

func (this *TaskStatQueue) lock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) unlock() {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}

func (this *TaskStatQueue) Pop() TaskStatInterface {
	this.lock()
	arrayLength := len(this.taskArray)
	if arrayLength == 0 {
		this.unlock()
		return nil
	}
	ret := this.taskArray[arrayLength-1]
	this.taskArray = this.taskArray[:arrayLength-1]
	this.unlock()
	return ret
}

func (this *TaskStatQueue) Push(taskStat TaskStatInterface) {
	this.lock()
	if taskStat == nil {
		this.unlock()
		return
	}
	this.taskArray = append(this.taskArray, taskStat)
	this.unlock()
}

func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	outArray := make([]TaskStatInterface, 0)
	this.lock()
	for taskIndex := 0; taskIndex < len(this.taskArray); {
		taskStat := this.taskArray[taskIndex]
		if (taskStat).OutOfTime() {
			outArray = append(outArray, taskStat)
			this.taskArray = append(this.taskArray[:taskIndex], this.taskArray[taskIndex+1:]...)
			// must resume at this index next time
		} else {
			taskIndex++
		}
	}
	this.unlock()
	return outArray
}

func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	this.lock()
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	this.lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	this.unlock()
}

type Coordinator struct {
	// Your definitions here.
	filenames []string

	//map任务队列
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	//reduce任务队列
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	//机器状态
	isDone  bool
	nReduce int
}

func (this *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.TaskType = EndTask
		return nil
	}
	// check for reduce tasks
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		reduceTask.SetNow()
		this.reduceTaskRunning.Push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduceTask on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}
	// check for map tasks
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		// an available map task
		// record task begin time
		mapTask.SetNow()
		// note task is running
		this.mapTaskRunning.Push(mapTask)
		// setup a reply
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}
	// all tasks distributed，只有前面任务都没了才会走到这里
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		// must wait for new tasks
		reply.TaskType = WaitTask
		return nil
	}

	// all tasks complete
	reply.TaskType = EndTask
	this.isDone = true
	return nil
}

func (this *Coordinator) distributeReduce() {
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   this.nReduce,
			nFiles:    len(this.filenames),
		},
	}
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		task := reduceTask
		task.partIndex = reduceIndex
		this.reduceTaskWaiting.Push(&task)
	}
}

func (this *Coordinator) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.TaskType {
	case MapTask:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// all map tasks done
			// can distribute reduce tasks
			this.distributeReduce()
		}
	case ReduceTask:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
	default:
		panic("Task Done error")
	}
	return nil
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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	mapArray := make([]TaskStatInterface, 0)
	for fileIndex, filename := range files {
		mapTask := MapTaskStat{
			TaskStat{
				fileName:  filename,
				fileIndex: fileIndex,
				partIndex: 0,
				nReduce:   nReduce,
				nFiles:    len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	c := Coordinator{
		mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		nReduce:        nReduce,
		filenames:      files,
	}

	go c.collectOutOfTime()

	// Your code here.

	c.server()
	return &c
}

func (this *Coordinator) collectOutOfTime() {
	for {
		time.Sleep(time.Duration(time.Second * 5))
		timeouts := this.reduceTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		timeouts = this.mapTaskRunning.TimeOutQueue()
		if len(timeouts) > 0 {
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}
