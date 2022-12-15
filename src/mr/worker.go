package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		response := doHeartBeat()
		switch response.TaskType {
		case MapTask:
			doMapTask(mapf, response)
		case ReduceTask:
			doReduceTask(reducef, response)
		case WaitTask:
			time.Sleep(5 * time.Second)
		case EndTask:
			fmt.Println("All tasks completed...")
			return
		default:
			panic("unexpected taskType received")
		}
	}

}

func doHeartBeat() *TaskInfo {
	args := ExampleArgs{}
	response := TaskInfo{}
	call("Coordinator.AskTask", &args, &response)
	return &response
}

func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Coordinator.TaskDone", taskInfo, &reply)
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

func doMapTask(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	fmt.Printf("Got %vth mapTask of %v file\n", taskInfo.FileIndex, taskInfo.FileName)
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kvs := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kvs...)

	// 将对应的kv输出到指定的文件中
	nReduce := taskInfo.NReduce
	outprefix := "/home/wxk/go/6.824/src/main/mr-tmp/mr-"
	outprefix += strconv.Itoa(taskInfo.FileIndex)
	outprefix += "-"
	outFiles := make([]*os.File, nReduce)
	fileEncoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFiles[i], _ = ioutil.TempFile("/home/wxk/go/6.824/src/main/mr-tmp", "mr-tmp-*")
		fileEncoders[i] = json.NewEncoder(outFiles[i])
	}
	for _, kv := range intermediate {
		outIndex := ihash(kv.Key) % nReduce
		file = outFiles[outIndex]
		encoder := fileEncoders[outIndex]
		err := encoder.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v key %v value %v ERROR: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	// 保存为文件
	for outIndex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outIndex)
		oldpath := filepath.Join(file.Name())
		os.Rename(oldpath, outname)
		file.Close()
	}

	// outprefix := "/home/wxk/go/6.824/src/main/mr-tmp/mr-"
	// outprefix += strconv.Itoa(taskInfo.FileIndex)
	// outprefix += "-"
	// reduces := make([][]KeyValue, taskInfo.NReduce)
	// for _, kv := range kvs {
	// 	idx := ihash(kv.Key) % taskInfo.NReduce
	// 	reduces[idx] = append(reduces[idx], kv)
	// }

	// for idx, l := range reduces {
	// 	outname := outprefix + strconv.Itoa(idx)
	// 	file, err := os.Create(outname)
	// 	if err != nil {
	// 		fmt.Printf("Error: %v\n", err)
	// 		panic("Create file failed")
	// 	}
	// 	encoder := json.NewEncoder(file)
	// 	for _, kv := range l {
	// 		if err := encoder.Encode(&kv); err != nil {
	// 			fmt.Printf("Error: %v\n", err)
	// 			panic("Json encode failed")
	// 		}
	// 	}
	// 	file.Close()
	// }

	// acknowledge master
	CallTaskDone(taskInfo)
}

func doReduceTask(reducef func(string, []string) string, taskInfo *TaskInfo) {
	fmt.Printf("Got the reduceTask of part %v\n", taskInfo.PartIndex)
	outname := "mr-out-" + strconv.Itoa(taskInfo.PartIndex)

	// 读取由mapTask产生的文件
	innameprefix := "/home/wxk/go/6.824/src/main/mr-tmp/mr-"
	innamesuffix := "-" + strconv.Itoa(taskInfo.PartIndex)

	intermediate := []KeyValue{}
	for i := 0; i < taskInfo.NFiles; i++ {
		fmt.Printf("NFiles:%v\n", taskInfo.NFiles)
		inname := innameprefix + strconv.Itoa(i) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()

	}

	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile("/home/wxk/go/6.824/src/main/mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}

	i := 0
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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()

	// acknowledge master
	CallTaskDone(taskInfo)
}
