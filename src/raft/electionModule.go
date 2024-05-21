package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogTerm int
	LogLen      int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Agree          bool
	CancelElection bool
	CurrentTerm    int
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(100+rand.Intn(400)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

// 选举函数
func (rf *Raft) leaderElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.VoteTerm = rf.currentTerm
	rf.resetElectionTimer()
	rf.persist()
	//fmt.Printf("Candidate%d 开始任期%d竞选\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LogLen:      len(rf.logs),
		LastLogTerm: rf.logs[len(rf.logs)-1].Term,
	}
	var voteCounter int32
	atomic.AddInt32(&voteCounter, 1)

	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(serverId, &args, &reply, &voteCounter)
		}
	}

	timeout := time.After(time.Duration(200) * time.Millisecond)
	for {
		select {
		case <-timeout:
			rf.state = Follower
			//fmt.Printf("Candidate%d选举超时\n", rf.me)
			return
		default:
			if rf.state == Follower {
				return
			}
			if voteCounter*2 > int32(len(rf.peers)) {
				//fmt.Printf("Candidate%d 得票：%d 竞选成功\n", rf.me, voteCounter)
				rf.state = Leader
				rf.leaderControl(true)
				return
			}
		}
	}
}

func (rf *Raft) leaderInit() {
	rf.resetElectionTimer()
	for server, _ := range rf.peers {
		if server != rf.me {
			rf.nextIndex[server] = len(rf.logs)
			rf.matchIndex[server] = 0
			go rf.beatStart(server)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteCounter *int32) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.Agree {
		atomic.AddInt32(voteCounter, 1)
	}
	if reply.CancelElection {
		rf.state = Follower
		rf.currentTerm = reply.CurrentTerm
	}
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		if args.Term > rf.VoteTerm { //本节点当前任期选举没投票，允许投票
			myLastLogTerm := rf.logs[len(rf.logs)-1].Term

			if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LogLen >= len(rf.logs)) {
				rf.mu.Lock()
				reply.Agree = true
				rf.VoteTerm = args.Term
				rf.mu.Unlock()
				//fmt.Printf("Follower%d向Candidate%d任期%d竞选投票\n", rf.me, args.CandidateId, args.Term)
			}
		}
		return
	}
	if args.Term < rf.currentTerm {
		reply.CancelElection = true
		reply.CurrentTerm = rf.currentTerm
		return
	}
}

// 示例代码，用于向服务器发送 RequestVote RPC。
// server 是目标服务器在 rf.peers[] 中的索引。
// 期望在 args 中传递 RPC 参数。
// 用 RPC 回复填充 *reply，因此调用者应传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与
// 处理函数中声明的参数的类型相同（包括它们是否是指针）。

// labrpc 包模拟了一个可能丢失的网络，在这个网络中，服务器可能无法达到，
// 请求和回复可能会丢失。Call() 发送请求并等待回复。如果在超时间隔内收到回复，
// Call() 返回 true；否则 Call() 返回 false。因此，Call() 可能需要一段时间才能返回。
// false 返回可能是由于服务器宕机、无法到达的活动服务器、丢失的请求或丢失的回复引起的。

// Call() 保证返回（可能会有延迟），*除非*服务器端的处理函数不返回。因此，
// 没有必要围绕 Call() 实现自己的超时。

// 查看 ../labrpc/labrpc.go 中的注释了解更多细节。

// 如果你在让 RPC 工作方面遇到困难，请检查你是否已经
// 将通过 RPC 传递的结构体中的所有字段名都大写，并且
// 调用者传递回复结构体的地址与 &，而不是结构体本身。
