package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		args := TaskGetRequest{}
		reply := TaskGetResponse{}
		CallTaskGet(&args, &reply)
		status := reply.Status
		if status == 0 && reply.XTask.Filename != "" {
			filename := reply.XTask.Filename
			id := strconv.Itoa(reply.XTask.MapId)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			bucket := make([][]KeyValue, reply.ReduceTaskNum)
			for _, kv := range kva {
				num := ihash(kv.Key) % reply.ReduceTaskNum
				bucket[num] = append(bucket[num], kv)
			}
			for i := 0; i < reply.ReduceTaskNum; i++ {
				tempFile, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					log.Fatalf("cannot open tmpMapOutFile")
				}
				enc := json.NewEncoder(tempFile)
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatalf("cannot encode")
				}
				tempFile.Close()
				outputFilename := "mr-" + id + "-" + strconv.Itoa(i)
				os.Rename(tempFile.Name(), outputFilename)
			}
			CallTaskDone(&TaskDoneRequest{
				X:  true,
				Id: reply.XTask.MapId,
			})
		} else if status == 1 {
			//reduce
			id := strconv.Itoa(reply.XTask.ReduceId)
			intermediate := []KeyValue{}
			for i := 0; i < reply.MapTaskNum; i++ {
				filenameMap := "mr-" + strconv.Itoa(i) + "-" + id
				inputFile, err := os.OpenFile(filenameMap, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", filenameMap)
				}
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}
			sort.Sort(ByKey(intermediate)) //kv切片中可能存在重复，所以排序，后面直接对比相邻位置就可以找出所有重复的key
			outputFilename := "mr-out-" + id
			tempFile, err := ioutil.TempFile("", "mr-reduce-*")
			//reduceOutputFile, err := os.OpenFile(oFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			if err != nil {
				log.Fatalf("cannot open tmpReduceOutFile")
			}
			i := 0
			for i < len(intermediate) { //遍历切片中的所有单词
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key { //找出所有相同的单词
					j++
				}
				values := []string{} //存放这些相同的单词
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values) //由于kv对v都是1，所以直接统计有多少个kv对就行

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			tempFile.Close()
			os.Rename(tempFile.Name(), outputFilename)
			CallTaskDone(&TaskDoneRequest{
				X:  false,
				Id: reply.XTask.ReduceId,
			})
		} else if status == 2 {
			break
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.

func CallTaskGet(args *TaskGetRequest, reply *TaskGetResponse) {
	// send the RPC request, wait for the reply.
	call("Master.TaskGet", &args, &reply)
}

func CallTaskDone(args *TaskDoneRequest) {
	reply := TaskDoneResponse{}
	call("Master.TaskDone", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()                 // 与服务端使用相同的socket文件路径
	c, err := rpc.DialHTTP("unix", sockname) // 通过Unix Socket连接RPC服务
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply) //发起RPC调用
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
