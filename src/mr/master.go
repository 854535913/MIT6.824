package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	Filename string
	MapId    int
	ReduceId int
}

type Master struct {
	// Your definitions here.
	Status           int32 //0:map 1:reduce 2:finish
	MapTaskNum       int   //有多少个map任务
	ReduceTaskNum    int   //有多少个reduce任务
	MapTaskChan      chan Task
	ReduceTaskChan   chan Task
	MapTaskStatus    sync.Map
	ReduceTaskStatus sync.Map
	files            []string
}

type TaskStatus struct {
	Time   int64
	IsDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) DoneGet(x bool) int {
	var i = 0
	if x {
		m.MapTaskStatus.Range(func(k, v interface{}) bool {
			if v.(TaskStatus).IsDone == true {
				i++
			}
			return true
		})
	} else {
		m.ReduceTaskStatus.Range(func(k, v interface{}) bool {
			if v.(TaskStatus).IsDone == true {
				i++
			}
			return true
		})
	}
	return i
}

func (m *Master) TaskGet(args *TaskGetRequest, reply *TaskGetResponse) error {
	status := atomic.LoadInt32(&m.Status)
	if status == 0 {
		if len(m.MapTaskChan) != 0 {
			maptask, ok := <-m.MapTaskChan
			if ok {
				reply.XTask = maptask
				m.MapTaskStatus.Store(maptask.MapId, TaskStatus{time.Now().Unix(), false})
			}
		}
	} else if status == 1 {
		if len(m.ReduceTaskChan) != 0 {
			reducetask, ok := <-m.ReduceTaskChan
			if ok {
				reply.XTask = reducetask
				m.ReduceTaskStatus.Store(reducetask.ReduceId, TaskStatus{time.Now().Unix(), false})
			}
		}
	}
	reply.MapTaskNum = m.MapTaskNum
	reply.ReduceTaskNum = m.ReduceTaskNum
	reply.Status = atomic.LoadInt32(&m.Status)
	return nil
}

func (m *Master) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	timeNow := time.Now().Unix()
	if args.X {
		if m.DoneGet(true) != m.MapTaskNum {
			tempTaskStatus, _ := m.MapTaskStatus.Load(args.Id) //检查这个完成的任务是否超时，超时直接丢弃不处理
			if timeNow-tempTaskStatus.(TaskStatus).Time > 10 {
				return nil
			}
			m.MapTaskStatus.Store(args.Id, TaskStatus{timeNow, true}) //没有超时，认为当前map任务正常完成，存储状态
			if m.DoneGet(true) == m.MapTaskNum {                      //如果完成的map数量和map总数一样，说明map全部完成了，应该结束map，开始reduce
				atomic.StoreInt32(&m.Status, 1)
				for i := 0; i < m.ReduceTaskNum; i++ {
					m.ReduceTaskChan <- Task{ReduceId: i}
					m.ReduceTaskStatus.Store(i, TaskStatus{time.Now().Unix(), false})
				}
			}
		}
	} else {
		if m.DoneGet(false) != m.ReduceTaskNum {
			tempTaskStatus, _ := m.ReduceTaskStatus.Load(args.Id)
			if timeNow-tempTaskStatus.(TaskStatus).Time > 10 {
				return nil
			}
			m.ReduceTaskStatus.Store(args.Id, TaskStatus{timeNow, true})
			if m.DoneGet(false) == m.ReduceTaskNum {
				atomic.StoreInt32(&m.Status, 2)
			}
		}
	}
	return nil
}

func (m *Master) TimeControl() {
	status := atomic.LoadInt32(&m.Status)
	timeNow := time.Now().Unix()
	if status == 0 {
		for i := 0; i < m.MapTaskNum; i++ {
			tempTaskStatus, _ := m.MapTaskStatus.Load(i)
			if !tempTaskStatus.(TaskStatus).IsDone && timeNow-tempTaskStatus.(TaskStatus).Time > 10 {
				//fmt.Println("map timeout")
				m.MapTaskChan <- Task{Filename: m.files[i], MapId: i}
				m.MapTaskStatus.Store(i, TaskStatus{timeNow, false})
			}
		}
	} else if status == 1 {
		for i := 0; i < m.ReduceTaskNum; i++ {
			tempTaskStatus, _ := m.ReduceTaskStatus.Load(i)
			if !tempTaskStatus.(TaskStatus).IsDone && timeNow-tempTaskStatus.(TaskStatus).Time > 10 {
				//fmt.Println("reduce timeout")
				m.ReduceTaskChan <- Task{ReduceId: i}
				m.ReduceTaskStatus.Store(i, TaskStatus{timeNow, false})
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m) //注册RPC服务
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)                  // 删除已存在的socket文件，以避免端口冲突
	l, e := net.Listen("unix", sockname) // 在Unix socket上监听
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) //启动了一个新的goroutine来处理通过设置的Unix Socket传入的HTTP请求。
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false //默认false

	// Your code here.
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		Status:         0,
		MapTaskNum:     len(files),
		ReduceTaskNum:  nReduce,
		MapTaskChan:    make(chan Task, len(files)),
		ReduceTaskChan: make(chan Task, nReduce),
		files:          files,
	}
	for i, file := range files {
		m.MapTaskChan <- Task{Filename: file, MapId: i}
		m.MapTaskStatus.Store(i, TaskStatus{time.Now().Unix(), false})
	}
	m.server()
	return &m
}
