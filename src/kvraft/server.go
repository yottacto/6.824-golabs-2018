package raftkv

import (
    "labgob"
    "labrpc"
    "log"
    "raft"
    "sync"
    "time"
)

const Debug = 0

const waitIndexIdleTime = 50 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

type OpName string

const (
    OpGet    OpName = "Get"
    OpPut    OpName = "Put"
    OpAppend OpName = "Append"
)

type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    Op    OpName
    Key   string
    Value string
}

type Result struct {
    committed chan struct{}
    value     string
}

type KVServer struct {
    sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    maxraftstate int // snapshot if log grows this big

    // Your definitions here.
    data         map[string]string
    result       map[int]*Result
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    cmd := Op{
        Op: OpGet,
        Key: args.Key,
    }
    reply.WrongLeader = false
    reply.Err = ""

    DPrintf("server <%d> handle [Get], key=[%s]", kv.me, args.Key)
    index, _, isLeader := kv.rf.Start(cmd)
    DPrintf("server <%d> send [Get] to raft, index=[%d]", kv.me, index)
    if !isLeader {
        DPrintf("server <%d> handle [Get], wrong leader", kv.me)
        reply.WrongLeader = true
        reply.Err = "Server is not the leader"
        return
    }

    local := make(chan struct{})
    kv.Lock()
    kv.result[index] = &Result{
        committed: local,
    }
    kv.Unlock()

    <-local
    kv.Lock()
    defer kv.Unlock()
    reply.Value = kv.result[index].value
    DPrintf("server <%d> handle [Get], cmd committed, value=[%s]", kv.me, reply.Value)
    delete(kv.result, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    cmd := Op{
        Op: func() OpName {
            if args.Op == "Put" {
                return OpPut
            }
            return OpAppend
        }(),
        Key: args.Key,
        Value: args.Value,
    }
    reply.WrongLeader = false
    reply.Err = ""

    DPrintf("server <%d> handle [%s], key=[%s], value=[%s]", kv.me, args.Op, args.Key, args.Value)
    index, _, isLeader := kv.rf.Start(cmd)
    if !isLeader {
        DPrintf("server <%d> handle [%s] wrong leader", kv.me, args.Op)
        reply.WrongLeader = true
        reply.Err = "Server is not the leader"
        return
    }

    kv.Lock()
    local := make(chan struct{})
    kv.result[index] = &Result{
        committed: local,
    }
    kv.Unlock()

    <-local
    kv.Lock()
    defer kv.Unlock()
    DPrintf("server <%d> handle [%s] key=[%s] value=[%s], cmd committed", kv.me, args.Op, args.Key, args.Value)
    delete(kv.result, index)
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

func (kv *KVServer) startLocalApplyProcess() {
    for {
        applyMsg := <-kv.applyCh
        index := applyMsg.CommandIndex
        cmd := applyMsg.Command.(Op)
        DPrintf("applying %v", applyMsg)
        if cmd.Op == OpPut {
            kv.Lock()
            kv.data[cmd.Key] = cmd.Value
            kv.Unlock()
        }
        if cmd.Op == OpAppend {
            kv.Lock()
            kv.data[cmd.Key] += cmd.Value
            kv.Unlock()
        }

        for {
            kv.Lock()
            if _, ok := kv.result[index]; ok {
                kv.Unlock()
                break
            }
            kv.Unlock()
            time.Sleep(waitIndexIdleTime)
        }

        kv.Lock()
        if cmd.Op == OpGet {
            kv.result[index].value = kv.data[cmd.Key]
        }
        kv.result[index].committed <- struct{}{}
        kv.Unlock()
    }
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
    kv.data = make(map[string]string)
    kv.result = make(map[int]*Result)

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.
    go kv.startLocalApplyProcess()

    return kv
}

