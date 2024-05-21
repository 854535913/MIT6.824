package raft

//
// 这是 raft 必须对外暴露给服务（或测试器）的 API 概要。
// 请参见下面每个函数的注释了解更多细节。
//
// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始就新的日志条目达成一致
// rf.GetState() (term, isLeader)
//   查询 Raft 的当前任期，以及它是否认为自己是领导者
// ApplyMsg
//   每当日志中提交了新条目，每个 Raft 节点应该向同一服务器中的服务（或测试器）发送一个 ApplyMsg。

import (
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/src/labrpc"

// import "bytes"
// import "../labgob"

// ApplyMsg 当每个 Raft 节点意识到连续的日志条目已被提交时，该节点应通过传递给 Make() 的
// applyCh 向同一服务器上的服务（或测试器）发送 ApplyMsg。
// 设置 CommandValid 为 true 以表明 ApplyMsg 包含一个新提交的日志条目。
// 在实验 3 中，你将希望通过 applyCh 发送其他类型的消息（例如，快照），
// 到那时，你可以向 ApplyMsg 添加字段，但对这些其他用途设置 CommandValid 为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftState int

const (
	Follower RaftState = iota // 默认状态为 Follower，iota 表示自增枚举值
	Candidate
	Leader
)

// 一个实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.Mutex          // 用于保护对该节点状态的共享访问的锁
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *Persister          // 用于存储该节点持久化状态的对象
	me        int                 // 该节点在 peers[] 中的索引
	dead      int32               // 由 Kill() 设置

	// 你的数据在这里（2A, 2B, 2C）。
	// 参考论文中的图 2，了解 Raft 服务器需要维护的状态描述。
	state        RaftState     //节点的角色
	currentTerm  int           //现在任期
	VoteTerm     int           //投给了谁
	logs         []LogEntry    //要记录的信息
	heartBeat    time.Duration //心跳周期
	electionTime time.Time     //选举周期
	commitIndex  int           //最高的已经被复制到大多数服务器的日志的索引
	lastApplied  int           //最高的已经被应用到状态机日志的索引
	nextIndex    []int         //记录leader下一次应该发送哪条日志给各个follower
	matchIndex   []int         //记录各个follower已成功复制的最高日志条目的索引
	applyChan    chan ApplyMsg
}

// 返回 currentTerm 和这个服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// 将 Raft 的持久状态保存到稳定存储中，
// 这样在崩溃和重启后可以恢复这些状态。
// 参见论文的图 2 了解哪些状态应该是持久的。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 在没有任何状态的情况下进行引导？
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 使用 Raft 的服务（例如 k/v 服务器）希望开始对将要追加到 Raft 日志中的下一条命令达成一致。
// 如果该服务器不是领导者，则返回 false。否则立即开始协商并返回。
// 不能保证此命令将永远提交到 Raft 日志中，因为领导者可能会失败或失去选举。即使 Raft 实例已被终止，此函数也应该优雅地返回。
// 第一个返回值是如果命令最终被提交，该命令将出现的索引。第二个返回值是当前的任期。第三个返回值是 true，如果这个服务器认为它是领导者。
type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	isLeader := true

	// Your code here (2B).
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Command: command, Term: rf.currentTerm}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs) - 1
	term = rf.currentTerm
	rf.matchIndex[rf.me] = index
	//fmt.Printf("Leader%d 收到log,任期%d\n", rf.me, rf.currentTerm)
	return index, term, isLeader
}

// 测试器在每次测试后不会停止 Raft 创建的 goroutines，但它会调用 Kill() 方法。
// 你的代码可以使用 killed() 来检查是否已调用 Kill()。使用原子操作避免了对锁的需求。
// 问题是长时间运行的 goroutines 会占用内存，并可能消耗 CPU 时间，可能导致后续测试失败并产生令人困惑的调试输出。
// 任何带有长时间运行循环的 goroutine 都应该调用 killed() 来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 服务或测试者想要创建一个 Raft 服务器。
// 所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 该服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。
// persister 是这个服务器保存其持久状态的地方，同时如果有的话，它最初也持有最近保存的状态。
// applyCh 是一个通道，测试者或服务期望 Raft 通过它发送 ApplyMsg 消息。
// Make() 必须迅速返回，因此它应该启动 goroutines 来执行任何长时间运行的工作。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.VoteTerm = -1
	rf.heartBeat = 100 * time.Millisecond
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = make([]LogEntry, 1) //log从1开始，0号位置为空
	rf.applyChan = applyCh

	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	return rf
}

// 状态检查函数
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.heartBeat)
		if rf.state == Leader {
			rf.leaderControl(false)
		}
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
	}
}
