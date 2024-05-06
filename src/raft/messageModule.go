package raft

type messageLeaderArgs struct {
	Term     int
	LeaderId int
}

type messageLeaderReply struct {
}

func (rf *Raft) messageLeader() {
	nowTerm := rf.currentTerm
	nowleaderID := rf.me
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}
		args := messageLeaderArgs{
			nowTerm,
			nowleaderID,
		}
		reply := messageLeaderReply{}
		go rf.messageLeaderSend(peer, &args, &reply)
	}
}

func (rf *Raft) AppendEntries(args *messageLeaderArgs, reply *messageLeaderReply) {
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.resetElectionTimer()
}

func (rf *Raft) messageLeaderSend(server int, args *messageLeaderArgs, reply *messageLeaderReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
