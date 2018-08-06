package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
)

var clients = make(map[int64]bool)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientID int64
	seqNo    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.seqNo = 1
	for {
		ck.clientID = nrand()
		if !clients[ck.clientID] {
			clients[ck.clientID] = true
			break
		}
	}
	DPrintf("Make clerk %d\n", ck.clientID)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("Clerk gets key %s\n", key)
	for {
		args := &GetArgs{key, ck.clientID, ck.seqNo}
		reply := new(GetReply)
		ck.leader %= len(ck.servers)
		done := make(chan bool, 1)

		go func() {
			ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
			done <- ok
		}()

		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader {
				ck.seqNo++
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
			ck.leader++
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if op == "Put" {
		DPrintf("Clerk puts key %s value %s\n", key, value)
	} else {
		DPrintf("Clerk appends key %s value %s\n", key, value)
	}

	for {
		args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, SeqNo: ck.seqNo}
		reply := new(PutAppendReply)

		ck.leader %= len(ck.servers)
		done := make(chan bool, 1)

		go func() {
			ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
			done <- ok
		}()

		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.seqNo++
				return
			}
			ck.leader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
