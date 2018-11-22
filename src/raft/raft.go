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
import "labrpc"

// import "bytes"
// import "labgob"

import (
    "time"
    "math/rand"
    "strconv"
)

// TODO timeout interval range, heartbeat interval
// in millisecond
const electionTimeoutLower = 500
const electionTimeoutUpper = 600
// TODO send heartbeat idle time only, not periodically
const heartbeatInterval = 100 * time.Millisecond
const commitApplyIdleInterval = 30 * time.Millisecond

func getRandomElectionTimeout() time.Duration {
    length := electionTimeoutUpper - electionTimeoutLower
    return time.Duration((rand.Intn(length) + electionTimeoutLower)) * time.Millisecond
}

type State string

const (
    Follower  State = "Follower"
    Candidate State = "Candidate"
    Leader    State = "Leader"
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    state          State
    id             string
    leaderID       string
    votedFor       string
    currentTerm    int
    lastHeartbeat  time.Time

    log            []LogEntry
    commitIndex    int
    lastApplied    int

    // leader state
    nextIndex      []int
    matchIndex     []int

    // other
    decommission   bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here (2A).
    term = rf.currentTerm
    isleader = rf.leaderID == rf.id
    return term, isleader
}

func (rf *Raft) transitionToFollower(newTerm int) {
    rf.state       = Follower
    rf.votedFor    = ""
    rf.leaderID    = ""
    rf.currentTerm = newTerm
}

func (rf *Raft) transitionToCandidate() {
    rf.state        = Candidate
    rf.currentTerm += 1
    rf.votedFor     = rf.id
    rf.leaderID     = ""
}

func (rf *Raft) promoteToLeader() {
    rf.Lock()
    defer rf.Unlock()

    DPrintf("server <%s> became leader\n", rf.id)

    rf.state = Leader
    rf.leaderID = rf.id

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        rf.nextIndex[i] = len(rf.log) + 1
        rf.matchIndex[i] = 0
        go rf.sendHeartbeat(rf.peers[i], i)
    }
}

func (rf *Raft) getLastEntryInfo() (int, int) {
    index := len(rf.log)
    if index == 0 {
        return 0, 0
    }
    return rf.log[index - 1].Term, index
}

func (rf *Raft) sendHeartbeat(end *labrpc.ClientEnd, i int) {
    ticker := time.NewTicker(heartbeatInterval)
    // FIXME ?
    rf.Lock()
    term := rf.currentTerm
    rf.Unlock()
    for {
        <-ticker.C
        rf.Lock()
        if rf.decommission {
            break
        }
        if rf.state == Leader && term == rf.currentTerm {
            DPrintf("server <%s, term=%d> sending heartbeat to server <%d>\n",
                rf.id, rf.currentTerm, i)
            go rf.sendAppendEntries(rf.peers[i], i, term)
        } else {
            break
        }
        rf.Unlock()
    }
    rf.Unlock()
}

func (rf *Raft) sendAppendEntries(end *labrpc.ClientEnd, i, term int) {
    requestName := "Raft.AppendEntries"

    for {
        rf.Lock()

        if term != rf.currentTerm || rf.state != Leader || rf.decommission {
            break
        }

        // FIXME according to Figure 2, `if last log index >= nextIndex for a
        // follower, send AppendEntries`, but will nextIndex > last log index?
        lastIndex := rf.nextIndex[i] - 1
        lastTerm := 0
        if lastIndex > 0 {
            lastTerm = rf.log[lastIndex - 1].Term
        }

        DPrintf("server <%s, term=%d> to server <%d>, rf.nextIndex=[%d], cmd=[%v]\n",
            rf.id, rf.currentTerm, i, rf.nextIndex[i], rf.log[lastIndex:])

        args := AppendEntriesArgs{
            Term: rf.currentTerm,
            LeaderID: rf.id,
            PrevLogIndex: lastIndex,
            PrevLogTerm: lastTerm,
            Entries: rf.log[lastIndex:],
            LeaderCommit: rf.commitIndex,
        }
        rf.Unlock()

        var reply AppendEntriesReply
        request := func() bool {
            return rf.peers[i].Call(requestName, &args, &reply)
        }
        if SendRPCRequest(requestName, request) {
            rf.Lock()
            DPrintf("server <%s, term=%d> got reply from sever <%d>, term=[%d], success=[%t]\n, cmd=[%v]",
                rf.id, args.Term, i, reply.Term, reply.Success, args.Entries)

            if args.Term != rf.currentTerm {
                break
            }

            if reply.Term > rf.currentTerm {
                rf.transitionToFollower(reply.Term)
                break
            }
            if reply.Success {
                rf.nextIndex[i] = len(rf.log) + 1
                rf.matchIndex[i] = rf.nextIndex[i] - 1
                break
            } else {
                // TODO optimization
                DPrintf("server <%s, term=%d> reduce server <%d>'s nextIndex\n",
                    rf.id, rf.currentTerm, i)
                rf.nextIndex[i] -= 1
            }
            rf.Unlock()
        }
    }
    rf.Unlock()
}

func (rf *Raft) startApplyProcess(applyCh chan ApplyMsg) {
    for {
        rf.Lock()
        if rf.commitIndex > rf.lastApplied {
            for i := rf.lastApplied; i < rf.commitIndex; i++ {
                DPrintf("server <%s> apply cmd [%v] at index [%d]\n",
                    rf.id, rf.log[i].Command, i + 1)
                applyCh <- ApplyMsg{
                    CommandValid: true,
                    Command: rf.log[i].Command,
                    CommandIndex: i + 1,
                }
            }
            rf.lastApplied = rf.commitIndex
        }

        if rf.state == Leader {
            DPrintf("server <%s> commitIndex [%d]\n",
                rf.id, rf.commitIndex)
            for n := len(rf.log); n > rf.commitIndex; n-- {
                if rf.log[n - 1].Term != rf.currentTerm {
                    break
                }
                count := 1
                for i, m := range rf.matchIndex {
                    if i == rf.me {
                        continue
                    }
                    if m >= n {
                        count += 1
                    }
                }
                if count > len(rf.peers) / 2 {
                    rf.commitIndex = n
                    break
                }
            }
        }
        rf.Unlock()

        time.Sleep(commitApplyIdleInterval)
    }
}

func (rf *Raft) startElectionProcess() {
    currentTimeout := getRandomElectionTimeout()
    currentTime := <-time.After(currentTimeout)

    rf.Lock()
    defer rf.Unlock()

    if rf.decommission {
        return
    }

    if rf.state != Leader && currentTime.Sub(rf.lastHeartbeat) > currentTimeout {
        DPrintf("server <%s> being election, it's random timeout is %s\n",
            rf.id, currentTimeout)
        go rf.beginElection()
    }
    go rf.startElectionProcess()
}

func (rf *Raft) beginElection() {
    rf.Lock()

    if rf.decommission {
        rf.Unlock()
        return
    }

    rf.transitionToCandidate()

    lastTerm, lastIndex := rf.getLastEntryInfo()
    args := RequestVoteArgs{
        Term: rf.currentTerm,
        CandidateID: rf.id,
        LastLogTerm: lastTerm,
        LastLogIndex: lastIndex,
    }

    npeers := len(rf.peers)
    vote := make(chan int, npeers)
    replies := make([]RequestVoteReply, npeers)
    for i := range rf.peers {
        if i != rf.me {
            go rf.sendRequestVote(rf.peers[i], i, vote, &args, &replies[i])
        }
    }
    rf.Unlock()

    supporter := 1
    // opponent := 0
    // TODO optimization for n/2 opponent
    // FIXME Unlock of unlocked mutex, when i == npeers - 1, will block on channel
    for i := 0; i < npeers; i++ {
        id := <-vote
        reply := replies[id]

        rf.Lock()

        // TODO same format
        DPrintf("server <%s> recieve reply from %d, Term is %d, VoteGranted is %t\n",
            rf.id, id, reply.Term, reply.VoteGranted)

        if args.Term != rf.currentTerm || rf.decommission {
            break
        }

        // TODO > or >=
        if reply.Term > rf.currentTerm {
            rf.transitionToFollower(reply.Term)
            break
        }
        if supporter += reply.VoteCount(); supporter > npeers / 2 {
            if rf.state == Candidate && args.Term == rf.currentTerm {
                go rf.promoteToLeader()
            }
            break
        }
        rf.Unlock()
    }
    rf.Unlock()
}

func (rf *Raft) sendRequestVote(
    end *labrpc.ClientEnd,
    i int,
    vote chan int,
    args *RequestVoteArgs,
    reply *RequestVoteReply,
) {
    DPrintf("server <%s>'s sendRequestVote to %d on term %d\n",
        args.CandidateID, i, args.Term)
    requestName := "Raft.RequestVote"
    request := func() bool {
        return rf.peers[i].Call(requestName, args, reply)
    }
    if SendRPCRequest(requestName, request) {
        DPrintf("server <%s, term=%d> got vote from sever <%d>, term=[%d], VoteGranted=[%t]\n",
            args.CandidateID, args.Term, i, reply.Term, reply.VoteGranted)

        vote <- i
    }
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.Lock()
    defer rf.Unlock()

    reply.Term = rf.currentTerm
    reply.Success = false

    if args.Term < rf.currentTerm {
        return
    } else {
        rf.transitionToFollower(args.Term)
        rf.leaderID = args.LeaderID
    }

    if rf.leaderID == args.LeaderID {
        rf.lastHeartbeat = time.Now()
    }

    if !rf.conflictLogAt(args.PrevLogIndex, args.PrevLogTerm) {
        DPrintf("AppendEntries: server <%s> conflict with server <%s>'s log at %d\n",
            args.LeaderID, rf.id, args.PrevLogIndex)
        return
    }

    // check entry confliction
    i := 0
    for ; i < len(args.Entries); i += 1 {
        if len(rf.log) <= i + args.PrevLogIndex {
            break
        }
        if rf.log[i + args.PrevLogIndex].Term != args.Entries[i].Term {
            args.Entries = args.Entries[i:]
            rf.log = rf.log[:i + args.PrevLogIndex]
            break
        }
    }
    args.Entries = args.Entries[i:]

    // append new entries
    rf.log = append(rf.log, args.Entries...)

    DPrintf("AppendEntries: server <%s> called from server <%s>, args=%v\n",
        rf.id, args.LeaderID, args)
    DPrintf("AppendEntries: server <%s> called from server <%s>, after append log=%v\n",
        rf.id, args.LeaderID, rf.log)

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
    }

    reply.Success = true
}

func (rf *Raft) conflictLogAt(index, term int) bool {
    if len(rf.log) < index {
        return false
    }
    if index == 0 {
        return term == 0
    }
    return rf.log[index - 1].Term == term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.Lock()
    defer rf.Unlock()

    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    if args.Term < rf.currentTerm {
        return
    }
    // FIXME >= ?
    if args.Term > rf.currentTerm {
        rf.transitionToFollower(args.Term)
    }
    if (len(rf.votedFor) == 0 || rf.votedFor == args.CandidateID) &&
        rf.atLeastAsUpToDate(args.LastLogTerm, args.LastLogIndex) {
        rf.votedFor = args.CandidateID
        reply.VoteGranted = true
        rf.lastHeartbeat = time.Now()
    }
}

func (rf *Raft) atLeastAsUpToDate(candLastLogTerm, candLastLogIndex int) bool {
    lastLogTerm, lastLogIndex := rf.getLastEntryInfo()
    if lastLogTerm != candLastLogTerm {
        return candLastLogTerm > lastLogTerm
    }
    return candLastLogIndex >= lastLogIndex
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//     ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//     return ok
// }


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
    if rf.state != Leader {
        return index, term, false
    }

    rf.Lock()
    defer rf.Unlock()

    rf.log = append(rf.log, LogEntry{
        Term: rf.currentTerm,
        Command: command,
    })
    term, index = rf.getLastEntryInfo()

    DPrintf("server <%s> start cmd [%v] at index=%d\n",
        rf.id, command, index)

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        go rf.sendAppendEntries(rf.peers[i], i, rf.currentTerm)
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
    rf.Lock()
    defer rf.Unlock()

    rf.decommission = true
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
func Make(
    peers []*labrpc.ClientEnd,
    me int,
    persister *Persister,
    applyCh chan ApplyMsg,
) *Raft {
    // Your initialization code here (2A, 2B, 2C).
    // rand.Seed(me)
    rf := &Raft{
        peers:         peers,
        persister:     persister,
        me:            me,
        id:            strconv.Itoa(me),
        lastHeartbeat: time.Now(),
        commitIndex:   0,
        lastApplied:   0,
        decommission:  false,
    }

    rf.transitionToFollower(0)
    go rf.startElectionProcess()
    go rf.startApplyProcess(applyCh)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}

