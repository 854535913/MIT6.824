package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Agree bool
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

// 选举函数
func (rf *Raft) leaderElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.voteInfo.VoteFor = rf.me
	rf.voteInfo.VoteTerm = rf.currentTerm
	rf.resetElectionTimer()
	rf.persist()
	fmt.Printf("竞选者%d 开始任期%d竞选\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	var mu sync.Mutex
	var voteCounter int32
	atomic.AddInt32(&voteCounter, 1)
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			reply := RequestVoteReply{
				Agree: false,
			}
			go rf.sendRequestVote(serverId, &args, &reply, &mu, voteCounter)
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		if rf.state == Candidate {
			rf.state = Follower
		}
		reply.Agree = false
		return
	}
	if args.Term != rf.voteInfo.VoteTerm || rf.voteInfo.VoteFor != args.CandidateId {
		rf.mu.Lock()
		reply.Agree = true
		rf.voteInfo.VoteFor = args.CandidateId
		rf.voteInfo.VoteTerm = args.Term
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, mu *sync.Mutex, voteCounter int32) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if reply.Agree {
		mu.Lock()
		atomic.AddInt32(&voteCounter, 1)
		mu.Unlock()
	}
	currentVotes := atomic.LoadInt32(&voteCounter)
	if currentVotes*2 > int32(len(rf.peers)) {
		fmt.Printf("竞选者%d 得票：%d 竞选成功\n", rf.me, currentVotes)
		rf.state = Leader
		rf.messageLeader()
	}
	return ok
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
