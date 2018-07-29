package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log Entry
type LogEntry struct {
	Term    int
	Command interface{}
}

// States
const (
	Leader = iota
	Follower
	Candidate
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile states on leaders
	nextIndex  []int
	matchIndex []int

	// Others
	state             int           // Leader, Follower or Candidate
	electionTimeout   time.Duration // 500~1000 ms
	electionTimer     *time.Timer
	heartbeatInterval time.Duration // interval between sending two heartbeats, 200 ms
	heartbeatTimer    *time.Timer
	logInterval       time.Duration // interval between applying two (groups of) log entries, 50 ms
	logTimer          *time.Timer
	applyCh           chan ApplyMsg
	commitCond        *sync.Cond // for commitIndex update
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d-%d-%d]: receive RequestVote from %d\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("[%d-%d-%d]: reject RequestVote from %d because of stale term\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
			DPrintf("[%d-%d-%d]: turn to Follower when handling RequestVote\n", rf.me, rf.state, rf.currentTerm)
		}

		if rf.votedFor == -1 {
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.electionTimer.Reset(rf.electionTimeout)
				reply.VoteGranted = true
				DPrintf("[%d-%d-%d]: accept RequestVote from %d\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
			} else {
				reply.VoteGranted = false
				DPrintf("[%d-%d-%d]: reject RequestVote from %d because of more up-to-date log\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
			}
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// arguments of AppendEntries RPC.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// reply of AppendEntries RPC.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d-%d-%d]: receive heartbeat from %d\n", rf.me, rf.state, rf.currentTerm, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if rf.state == Leader {
		rf.state = Follower
		rf.votedFor = -1
		rf.electionTimer = time.NewTimer(rf.electionTimeout)
		go rf.launchElections()
	} else {
		rf.electionTimer.Reset(rf.electionTimeout)
	}
}

//
// send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		logEntry := LogEntry{rf.currentTerm, command}
		rf.log = append(rf.log, logEntry)
		index = len(rf.log) - 1
		term = rf.currentTerm
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("[%d-%d-%d]: add new log entry at index %d\n", rf.me, rf.state, rf.currentTerm, index)
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{0, nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.electionTimeout = time.Millisecond * time.Duration(500+rand.Intn(500))
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.heartbeatInterval = time.Millisecond * 200
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatInterval)
	rf.logInterval = time.Millisecond * 50
	rf.logTimer = time.NewTimer(rf.logInterval)
	rf.applyCh = applyCh
	rf.commitCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.launchElections()
	go rf.sendApplyMsgs()

	return rf
}

//
// launch election on server initialization
//
func (rf *Raft) launchElections() {
	for {
		<-rf.electionTimer.C

		if _, isLeader := rf.GetState(); isLeader {
			// Only non-Leader can launch elections
			DPrintf("[%d-%d-%d]: stop launching election\n", rf.me, rf.state, rf.currentTerm)
			rf.electionTimer.Stop()
			return
		}
		DPrintf("[%d-%d-%d]: election timeout\n", rf.me, rf.state, rf.currentTerm)
		go rf.requestVotes()
		rf.electionTimer.Reset(rf.electionTimeout)
	}
}

//
// request votes from all other servers when launching election
//
func (rf *Raft) requestVotes() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	DPrintf("[%d-%d-%d]: turn to Candidate\n", rf.me, rf.state, rf.currentTerm)
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}
	rf.mu.Unlock()

	numVotes := 1
	numPeers := len(rf.peers)
	for i := 0; i < numPeers; i++ {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				DPrintf("[%d-%d-%d]: send RequestVote to %d\n", rf.me, rf.state, rf.currentTerm, i)
				if rf.sendRequestVote(i, &args, &reply) {
					// Handle RequestVote RPC reply
					rf.mu.Lock()
					if rf.state == Candidate {
						if reply.Term > args.Term {
							// Candidate has stale term, turns to Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.electionTimer.Reset(rf.electionTimeout)
							DPrintf("[%d-%d-%d]: from Candidate to Follower\n", rf.me, rf.state, rf.currentTerm)
						} else if reply.VoteGranted {
							numVotes++
							if numVotes > numPeers/2 {
								// Candidate is elected as Leader
								rf.state = Leader
								rf.electionTimer.Reset(rf.electionTimeout)
								for j := 0; j < numPeers; j++ {
									rf.nextIndex[j] = len(rf.log)
									if j == rf.me {
										rf.matchIndex[j] = len(rf.log) - 1
									} else {
										rf.matchIndex[j] = 0
									}
								}
								rf.heartbeatTimer = time.NewTimer(rf.heartbeatInterval)
								rf.logTimer = time.NewTimer(rf.logInterval)
								go rf.sendHeartbeats()
								go rf.sendLogs()
								DPrintf("[%d-%d-%d]: new Leader\n", rf.me, rf.state, rf.currentTerm)
							}
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

//
// send heartbeats to all other servers
//
func (rf *Raft) sendHeartbeats() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			// Only Leader can send heartbeats
			rf.heartbeatTimer.Stop()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("[%d-%d-%d]: send heartbeat to %d\n", rf.me, rf.state, rf.currentTerm, i)
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, Entries: nil, LeaderCommit: rf.commitIndex}
					var reply AppendEntriesReply
					if rf.sendAppendEntries(i, &args, &reply) {
						// Handle AppendEntries RPC reply
						DPrintf("[%d-%d-%d]: handle heartbeat reply from %d\n", rf.me, rf.state, rf.currentTerm, i)
						if rf.state == Leader && reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.electionTimer = time.NewTimer(rf.electionTimeout)
							go rf.launchElections()
						}
					}
				}(i)
			}
		}
		rf.heartbeatTimer.Reset(rf.heartbeatInterval)
		<-rf.heartbeatTimer.C
	}
}

//
// send new logs to all other servers
//
func (rf *Raft) sendLogs() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			// Only Leader can send logs
			rf.logTimer.Stop()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if len(rf.log) > rf.nextIndex[i] {
						DPrintf("[%d-%d-%d]: send logs to %d\n", rf.me, rf.state, rf.currentTerm, i)
						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, Entries: make([]LogEntry, len(rf.log)-rf.nextIndex[i]), LeaderCommit: rf.commitIndex}
						copy(args.Entries, rf.log[rf.nextIndex[i]:])
						var reply AppendEntriesReply
						if rf.sendAppendEntries(i, &args, &reply) {
							// Handle AppendEntries RPC reply
							DPrintf("[%d-%d-%d]: handle AppendEntries reply from %d\n", rf.me, rf.state, rf.currentTerm, i)
							if rf.state == Leader && reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.votedFor = -1
								rf.electionTimer = time.NewTimer(rf.electionTimeout)
								go rf.launchElections()
							}

						}
					}
				}(i)
			}
		}
		rf.logTimer.Reset(rf.logInterval)
		<-rf.logTimer.C
	}
}

//
// send apply messages to tester or service
//
func (rf *Raft) sendApplyMsgs() {
	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
		}
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
			}
		}
		rf.mu.Unlock()
	}
}
