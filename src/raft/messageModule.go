package raft

import (
	"fmt"
	"sort"
)

type emptyReply struct {
}

type AppendModule int

const (
	Beat AppendModule = iota
	Sync
	Commit
)

type leaderArgs struct {
	Module         AppendModule
	CurrentTerm    int
	LeaderId       int
	LeaderMaxIndex int //leader最大的index
	LeaderCommit   int //leader最高的已经被复制到大多数服务器的日志的索引
	PrevLogIndex   int //预计从follower哪里开始追加
	PrevLogTerm    int
	Logs           []LogEntry //要存储的log
}

type followerReply struct {
	CurrentTerm int                // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool               // 如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppendState AppendEntriesState // 追加状态
	UpNextIndex int
}

type AppendEntriesState int

const (
	AppendSuccess AppendEntriesState = iota // 追加正常
	LeaderExpire
	Mismatch // 追加不匹配
)

func (rf *Raft) leaderControl(isSync bool) {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go rf.beatStart(peer)

		if len(rf.logs)-1 > rf.matchIndex[peer] {
			fmt.Printf("Leader%d 发起复制\n", rf.me)
			go rf.syncStart(peer)
		}

		matchIndexes := make([]int, len(rf.matchIndex))
		copy(matchIndexes, rf.matchIndex)
		sort.Ints(matchIndexes)
		N := matchIndexes[len(matchIndexes)-(len(rf.peers)/2+1)]
		if N > rf.commitIndex {
			rf.commitIndex = N
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[N].Command,
				CommandIndex: N,
			}
			rf.applyChan <- applyMsg

			fmt.Printf("Leader%d 完成commit 并向follower发起commit\n", rf.me)
			fmt.Printf("commitIndex:%d log:%d term:%d\n", rf.commitIndex, rf.logs[rf.commitIndex].Command, rf.logs[rf.commitIndex].Term)
			for peer, _ := range rf.peers {
				if peer == rf.me {
					continue
				}
				go rf.commitStart(peer)
			}
		}
	}
}

func (rf *Raft) beatStart(server int) {
	args := leaderArgs{
		Module:      Beat,
		CurrentTerm: rf.currentTerm,
		LeaderId:    rf.me,
	}
	reply := emptyReply{}
	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
}

func (rf *Raft) syncStart(server int) {
	args := leaderArgs{
		Module:         Sync,
		CurrentTerm:    rf.currentTerm,
		LeaderId:       rf.me,
		LeaderMaxIndex: len(rf.logs) - 1,
		LeaderCommit:   rf.commitIndex,
		PrevLogIndex:   rf.matchIndex[server],
		PrevLogTerm:    rf.logs[rf.matchIndex[server]].Term,
	}
	for i := rf.nextIndex[server]; i <= args.LeaderMaxIndex; i++ {
		args.Logs = append(args.Logs, rf.logs[i])
	}
	reply := followerReply{
		Success: false,
	}
	for {
		rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if reply.Success {
			rf.matchIndex[server] = args.LeaderMaxIndex
			break
		} else {
			//follower拒绝同步，检查原因并重试
			switch reply.AppendState {
			//本节点不应该是leader了，退回follower
			case LeaderExpire:
				rf.state = Follower
				rf.currentTerm = reply.CurrentTerm
				rf.resetElectionTimer()
			//PrevLogIndex不匹配，向前一步重试
			case Mismatch:
				newLog := append([]LogEntry{rf.logs[args.PrevLogIndex]}, args.Logs...)
				args.PrevLogIndex = reply.UpNextIndex
				args.PrevLogTerm = rf.logs[reply.UpNextIndex].Term
				args.Logs = newLog
			}
		}
	}
}

func (rf *Raft) commitStart(server int) {
	args := leaderArgs{
		Module:       Commit,
		CurrentTerm:  rf.currentTerm,
		LeaderCommit: rf.commitIndex,
	}
	reply := emptyReply{}
	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
}

func (rf *Raft) AppendEntries(args *leaderArgs, reply *followerReply) {
	rf.currentTerm = args.CurrentTerm
	rf.state = Follower
	rf.resetElectionTimer()
	if args.Module == Sync {
		//如果leader任期小于当前follower，拒绝同步，并且让leader不再继续尝试同步
		if rf.currentTerm > args.CurrentTerm {
			reply.CurrentTerm = rf.currentTerm
			reply.Success = false
			reply.AppendState = LeaderExpire
			return
		}
		//如果本follower在PrevLogIndex位置的term与PrevLogTerm不一致，拒绝同步，要求leader将PrevLogIndex往前
		if len(rf.logs) != 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.AppendState = Mismatch
			reply.UpNextIndex = args.PrevLogIndex - 1
			return
		}
		//同意并开始进行同步
		fmt.Printf("Follower%d 开始复制\n", rf.me)
		for i, j := args.PrevLogIndex+1, 0; j < len(args.Logs); i, j = i+1, j+1 {
			if i < len(rf.logs) {
				rf.logs[i] = args.Logs[j]
			} else {
				rf.logs = append(rf.logs, args.Logs[j])
			}
		}
		reply.Success = true
		reply.AppendState = AppendSuccess
		fmt.Printf("Follower%d 完成复制\n", rf.me)
	}
	if args.Module == Commit {
		if args.LeaderCommit > rf.commitIndex {
			fmt.Printf("Follower%d 开始commit\n", rf.me)

			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex].Command,
				CommandIndex: rf.commitIndex,
			}
			rf.applyChan <- applyMsg

			fmt.Printf("commitIndex:%d %d\n", rf.commitIndex, rf.logs)
		}
	}
}
