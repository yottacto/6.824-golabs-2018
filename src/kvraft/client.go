package raftkv

import (
    "labrpc"
    "crypto/rand"
    "math/big"
 )

type Clerk struct {
    servers []*labrpc.ClientEnd
    // You will have to modify this struct.
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
    DPrintf("Get: key=[%s]\n", key)
    lastServer := 0
    requestName := "KVServer.Get"
    args := GetArgs{
        Key: key,
    }
    reply := GetReply{}
    request := func() bool {
        reply = GetReply{}
        return ck.servers[lastServer].Call(requestName, &args, &reply)
    }
    for {
        DPrintf("client sending <Get> to <%d>, key is [%s]\n", lastServer, key)
        if SendRPCRequest(requestName, request) {
            DPrintf("client got reply from <%d>\n", lastServer)
            if reply.WrongLeader {
                DPrintf("the server is not the leader")
                lastServer = (lastServer + 1) % len(ck.servers)
            } else {
                break
            }
        }
    }
    return reply.Value
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

    DPrintf("PutAppend: op=[%s], key=[%s], value=[%s]\n", op, key, value)
    lastServer := 0
    requestName := "KVServer.PutAppend"
    args := PutAppendArgs{
        Key:   key,
        Value: value,
        Op:    op,
    }
    reply := PutAppendReply{}
    request := func() bool {
        reply = PutAppendReply{}
        return ck.servers[lastServer].Call(requestName, &args, &reply)
    }
    for {
        DPrintf("client sending <%s> to <%d>, key=[%s], value=[%s]\n", op, lastServer, key, value)
        if SendRPCRequest(requestName, request) {
            DPrintf("client got reply from <%d> for <%s>\n", lastServer, op)
            if reply.WrongLeader {
                DPrintf("the server is not the leader")
                lastServer = (lastServer + 1) % len(ck.servers)
            } else {
                break
            }
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}

