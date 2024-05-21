package raft

import (
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
	CommitIndex    int //leader最高的已经被复制到大多数服务器的日志的索引
	PrevLogIndex   int //预计从follower哪里开始追加
	PrevLogTerm    int
	Logs           []LogEntry //要存储的log
}

type followerReply struct {
	CurrentTerm        int // leader的term可能是过时的，此时收到的Term用于更新他自己
	LeaderStatusExpire bool
	Success            bool               // 如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppendState        AppendEntriesState // 追加状态
	UpNextIndex        int
}

type AppendEntriesState int

const (
	AppendSuccess AppendEntriesState = iota // 追加正常
	LeaderExpire
	Mismatch // 追加不匹配
	Empty
)

func (rf *Raft) leaderControl(electionFin bool) {
	for server, _ := range rf.peers {
		if server == rf.me {
			rf.resetElectionTimer()
			continue
		}
		go rf.beatStart(server)

		if len(rf.logs)-1 > rf.matchIndex[server] {
			if rf.state != Leader {
				return
			}
			//fmt.Printf("Leader%d 向Server%d 发起复制\n", rf.me, server)
			go rf.syncStart(server)
		}
	}

	//commit
	if rf.state != Leader {
		return
	}
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)
	majorMatch := sortedMatchIndex[(len(rf.peers) / 2)]

	if majorMatch > rf.commitIndex {
		rf.commitIndex = majorMatch
		for rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			rf.lastApplied++
			//fmt.Printf("Server%d 开始apply index:%d\n", rf.me, rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyChan <- applyMsg
			rf.mu.Unlock()
		}

		//fmt.Printf("Leader%d 完成commit 并向follower发起commit\n", rf.me)
		//fmt.Printf("commitIndex:%d log:%d term:%d\n", rf.commitIndex, rf.logs[rf.commitIndex].Command, rf.logs[rf.commitIndex].Term)
		for server, _ := range rf.peers {
			if server != rf.me {
				go rf.commitStart(server)
			}
		}
	}
}

func (rf *Raft) beatStart(server int) {
	if rf.state != Leader {
		return
	}
	args := leaderArgs{
		Module:      Beat,
		CurrentTerm: rf.currentTerm,
		LeaderId:    rf.me,
	}
	reply := followerReply{}

	if rf.peers[server].Call("Raft.AppendEntries", &args, &reply); reply.LeaderStatusExpire {
		rf.state = Follower
		rf.currentTerm = reply.CurrentTerm
	}
}

func (rf *Raft) syncStart(server int) {
	if rf.state != Leader {
		return
	}
	args := leaderArgs{
		Module:         Sync,
		CurrentTerm:    rf.currentTerm,
		LeaderId:       rf.me,
		LeaderMaxIndex: len(rf.logs) - 1,
		CommitIndex:    rf.commitIndex,
		PrevLogIndex:   rf.matchIndex[server],
		PrevLogTerm:    rf.logs[rf.matchIndex[server]].Term,
	}
	for i := rf.nextIndex[server]; i <= args.LeaderMaxIndex; i++ {
		args.Logs = append(args.Logs, rf.logs[i])
	}
	reply := followerReply{}
	for {
		rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if reply.LeaderStatusExpire {
			rf.state = Follower
			rf.currentTerm = reply.CurrentTerm
			rf.resetElectionTimer()
			return
		}
		if rf.state != Leader {
			return
		}
		if reply.Success {
			rf.matchIndex[server] = args.LeaderMaxIndex
			rf.nextIndex[server] = args.LeaderMaxIndex + 1
			return
		} else {
			//follower拒绝同步，检查原因并重试
			switch reply.AppendState {
			//本节点不应该是leader了，退回follower
			case LeaderExpire:
				rf.state = Follower
				rf.currentTerm = reply.CurrentTerm
				rf.resetElectionTimer()
				return
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
	if rf.state != Leader {
		return
	}
	args := leaderArgs{
		Module:      Commit,
		CurrentTerm: rf.currentTerm,
		CommitIndex: rf.commitIndex,
		LeaderId:    rf.me,
	}
	reply := followerReply{}
	rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if reply.LeaderStatusExpire {
		rf.state = Follower
		rf.currentTerm = reply.CurrentTerm
	}
}

func (rf *Raft) AppendEntries(args *leaderArgs, reply *followerReply) {
	rf.resetElectionTimer()
	if rf.currentTerm < args.CurrentTerm { //接收心跳的节点，发现自己的任期小于发送者，退回follower
		//fmt.Printf("Server%d 发现自己过时,同步CurrentTerm\n", rf.me)
		rf.state = Follower
		rf.currentTerm = args.CurrentTerm
		return
	}
	if args.CurrentTerm < rf.currentTerm { //接收心跳的节点，发现发送者的任期小于自己，要求发送者退回follower
		//fmt.Printf("Server%d 发现发送者%d过时,要求其退回follower\n", rf.me, args.LeaderId)
		reply.CurrentTerm = rf.currentTerm
		reply.LeaderStatusExpire = true
		return
	}

	if args.Module == Sync {
		//如果leader任期小于当前follower，拒绝同步，并且让leader不再继续尝试同步
		if rf.currentTerm > args.CurrentTerm {
			reply.CurrentTerm = rf.currentTerm
			reply.LeaderStatusExpire = true
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
		//fmt.Printf("Follower%d 开始复制\n", rf.me)
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Logs...)
		reply.Success = true
		reply.AppendState = AppendSuccess
		//fmt.Printf("Follower%d 完成复制\n", rf.me)
	}
	if args.Module == Commit {
		if args.CommitIndex > rf.commitIndex {
			//fmt.Printf("Follower%d 开始commit\n", rf.me)

			rf.commitIndex = min(args.CommitIndex, len(rf.logs)-1)
			for rf.lastApplied < rf.commitIndex {
				rf.mu.Lock()
				rf.lastApplied++
				//fmt.Printf("Server%d 开始apply index:%d\n", rf.me, rf.lastApplied)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.mu.Unlock()
			}

			//fmt.Printf("follower%d commitIndex:%d %d\n", rf.me, rf.commitIndex, rf.logs)
		}
	}
}
