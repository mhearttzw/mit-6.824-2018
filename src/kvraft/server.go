package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Get", "Put", "Append"
	Key      string
	Value    string
	ClientID int64
	SeqNo    int
}

type LastReply struct {
	SeqNo int
	Reply GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs             map[string]string
	notifyChs       map[int]chan struct{}
	lastClientReply map[int64]*LastReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, leader := kv.rf.GetState(); !leader {
		DPrintf("Non-Leader %d receives Get %s", kv.me, args.Key)
		reply.WrongLeader = true
		reply.Err = ""
		reply.Value = ""
		return
	}

	kv.mu.Lock()
	if lastReply, ok := kv.lastClientReply[args.ClientID]; ok {
		if args.SeqNo <= lastReply.SeqNo {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = ""
			reply.Value = lastReply.Reply.Value
			return
		}
	}

	command := Op{Op: "Get", Key: args.Key, ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(command)

	ch := make(chan struct{})
	kv.notifyChs[index] = ch

	<-ch
	currentTerm, isLeader := kv.rf.GetState()
	// what if still leader, but different term? let client retry
	if !isLeader || term != currentTerm {
		reply.WrongLeader = true
		reply.Err = ""
		reply.Value = ""
		return
	}

	kv.mu.Lock()
	if value, ok := kv.kvs[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.notifyChs = make(map[int]chan struct{})
	kv.lastClientReply = make(map[int64]*LastReply)

	return kv
}
