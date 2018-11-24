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

import (
    "sync"
    "labrpc"
    "bytes"
    "labgob"
    "time"
    "math/rand"
    "strconv"
)

// TODO timeout interval range, heartbeat interval
// in millisecond
const electionTimeoutLower = 200
const electionTimeoutUpper = 500
// TODO send heartbeat idle time only, not periodically
const heartbeatInterval = 100 * time.Millisecond
const leaderPeerInterval = 5 * time.Millisecond
const commitApplyIdleInterval = 25 * time.Millisecond

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
    appendChan     []chan struct{}
}

type RaftPersistentState struct {
    CurrentTerm int
    VotedFor    string
    Log         []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here (2A).
    term = rf.currentTerm
    isleader = rf.state == Leader
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
    rf.votedFor     = rf.id
    rf.leaderID     = ""
    rf.currentTerm++
}

func (rf *Raft) promoteToLeader() {
    rf.Lock()
    defer rf.Unlock()

    DPrintf("server <%s> became leader\n", rf.id)

    rf.state = Leader
    rf.leaderID = rf.id

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    rf.appendChan = make([]chan struct{}, len(rf.peers))

    for i := range rf.peers {
        if i == rf.me {
            continue
        }
        rf.appendChan[i] = make(chan struct{}, 1)
        rf.nextIndex[i] = len(rf.log) + 1
        rf.matchIndex[i] = 0
        go rf.startLeaderPeerProcess(i, rf.appendChan[i])
    }
}

func (rf *Raft) startLeaderPeerProcess(peerIndex int, peerAppendChan chan struct{}) {
    ticker := time.NewTicker(leaderPeerInterval)

    // heartbeat
    rf.sendAppendEntries(peerIndex, peerAppendChan)
    lastEntrySent := time.Now()

    for {
        rf.Lock()
        if rf.state != Leader || rf.decommission {
            ticker.Stop()
            rf.Unlock()
            break
        }
        rf.Unlock()

        select {
        case <-peerAppendChan:
            lastEntrySent = time.Now()
            rf.sendAppendEntries(peerIndex, peerAppendChan)
        case currentTime := <-ticker.C:
            if currentTime.Sub(lastEntrySent) >= heartbeatInterval {
                lastEntrySent = time.Now()
                rf.sendAppendEntries(peerIndex, peerAppendChan)
            }
        }
    }
}

func (rf *Raft) getLastEntryInfo() (int, int) {
    index := len(rf.log)
    if index == 0 {
        return 0, 0
    }
    return rf.log[index - 1].Term, index
}

func (rf *Raft) sendAppendEntries(i int, peerAppendChan chan struct{}) {
    rf.Lock()
    if rf.state != Leader || rf.decommission {
        rf.Unlock()
        return
    }

    // FIXME according to Figure 2, `if last log index >= nextIndex for a
    // follower, send AppendEntries`, but will nextIndex > last log index?
    lastIndex := rf.nextIndex[i] - 1
    lastTerm := 0
    if lastIndex > 0 {
        lastTerm = rf.log[lastIndex - 1].Term
    }

    entries := make([]LogEntry, len(rf.log) - lastIndex)
    copy(entries, rf.log[lastIndex:])

    args := AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderID:     rf.id,
        PrevLogIndex: lastIndex,
        PrevLogTerm:  lastTerm,
        Entries:      entries,
        LeaderCommit: rf.commitIndex,
    }

    DPrintf("server <%s, term=%d> sent to server <%d>, len(entries)=[%d]",
        rf.id, args.Term, i, len(args.Entries))

    newNextIndex := len(rf.log) + 1
    // oldNextIndex := rf.nextIndex[i]
    rf.Unlock()

    reply := AppendEntriesReply{}
    requestName := "Raft.AppendEntries"
    request := func() bool {
        reply = AppendEntriesReply{}
        return rf.peers[i].Call(requestName, &args, &reply)
    }

    ok := SendRPCRequest(requestName, request)

    rf.Lock()
    defer rf.Unlock()
    if ok {
        DPrintf("server <%s, term=%d> got reply from server <%d>, term=[%d], success=[%t]",
            rf.id, args.Term, i, reply.Term, reply.Success)

        if args.Term != rf.currentTerm || rf.state != Leader || rf.decommission {
            return
        }

        if reply.Term > rf.currentTerm {
            rf.transitionToFollower(reply.Term)
            return
        }

        if reply.Success {
            rf.nextIndex[i] = newNextIndex
            rf.matchIndex[i] = newNextIndex - 1
            rf.updateCommitIndex()
            return
        } else {
            DPrintf("Log deviation: from server <%s, term=%d> to server <%d>, args.[T: %d, I: %d], reply.[Term: %d, ConflictLogTerm: %d, ConflictLogIndex: %d], prev nextIndex=[%d], reduced nextIndex=[%d], \n",
                rf.id, rf.currentTerm, i, args.PrevLogTerm, args.PrevLogIndex, reply.Term, reply.ConflictLogTerm, reply.ConflictLogIndex, rf.nextIndex[i], Max(1, reply.ConflictLogIndex-1))
            // TODO need a loop to find first term not match
            rf.nextIndex[i] = Max(1, reply.ConflictLogIndex - 1)
            peerAppendChan <- struct{}{}
        }
    }
    rf.persist()
}

func (rf *Raft) updateCommitIndex() {
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
                if count++; count > len(rf.peers) / 2 {
                    rf.commitIndex = n
                    break
                }
            }
        }
    }
}

func (rf *Raft) startApplyProcess(applyCh chan ApplyMsg) {
    for {
        rf.Lock()
        if rf.commitIndex > rf.lastApplied {
            // TODO imporve liveness
            entries := make([]LogEntry, rf.commitIndex - rf.lastApplied)
            copy(entries, rf.log[rf.lastApplied : rf.commitIndex])

            startIndex := rf.lastApplied + 1
            serverID := rf.id
            rf.Unlock()

            for i, l := range entries {
                DPrintf("server <%s> apply cmd [%v] at index [%d]\n",
                    serverID, l.Command, startIndex + i)
                applyCh <- ApplyMsg{
                    CommandValid: true,
                    Command: l.Command,
                    CommandIndex: startIndex + i,
                }
            }

            rf.Lock()
            rf.lastApplied += len(entries)
            rf.Unlock()
        } else {
            rf.Unlock()
            time.Sleep(commitApplyIdleInterval)
        }
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

    if rf.state != Leader && currentTime.Sub(rf.lastHeartbeat) >= currentTimeout {
        DPrintf("server <%s> being election, it's random timeout is [%s]\n",
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
    rf.lastHeartbeat = time.Now()

    lastTerm, lastIndex := rf.getLastEntryInfo()
    args := RequestVoteArgs{
        Term:         rf.currentTerm,
        CandidateID:  rf.id,
        LastLogTerm:  lastTerm,
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
    rf.persist()
    rf.Unlock()

    supporter := 1
    // opponent := 0
    // TODO optimization for n/2 opponent
    // FIXME Unlock of unlocked mutex, when i == npeers - 1, will block on channel
    for i := 0; i < npeers; i++ {
        id := <-vote
        reply := replies[id]

        rf.Lock()

        if args.Term != rf.currentTerm || rf.decommission {
            break
        }

        // TODO > or >=
        if reply.Term > rf.currentTerm {
            rf.transitionToFollower(reply.Term)
            break
        }
        if supporter += reply.VoteCount(); supporter > npeers / 2 {
            if rf.state == Candidate {
                go rf.promoteToLeader()
            }
            break
        }
        rf.Unlock()
    }
    rf.persist()
    rf.Unlock()
}

func (rf *Raft) sendRequestVote(
    end *labrpc.ClientEnd,
    i int,
    vote chan int,
    args *RequestVoteArgs,
    reply *RequestVoteReply,
) {
    DPrintf("server <%s>'s sendRequestVote to server <%d> on term [%d]\n",
        args.CandidateID, i, args.Term)
    requestName := "Raft.RequestVote"
    request := func() bool {
        *reply = RequestVoteReply{}
        return rf.peers[i].Call(requestName, args, reply)
    }
    if SendRPCRequest(requestName, request) {
        DPrintf("server <%s, term=%d> got vote from server <%d>, term=[%d], VoteGranted=[%t]\n",
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
    buf := new(bytes.Buffer)
    labgob.NewEncoder(buf).Encode(RaftPersistentState{
        CurrentTerm: rf.currentTerm,
        VotedFor: rf.votedFor,
        Log: rf.log,
    })
    rf.persister.SaveRaftState(buf.Bytes())
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    // Your code here (2C).
    buf := bytes.NewBuffer(data)
    obj := RaftPersistentState{}
    // TODO check decode error?
    labgob.NewDecoder(buf).Decode(&obj)
    rf.currentTerm, rf.votedFor, rf.log = obj.CurrentTerm, obj.VotedFor, obj.Log
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.Lock()
    defer rf.Unlock()

    DPrintf("AppendEntries: server <%s> request from server <%s>, len(entries)=[%d], args.Prev[Term: %d, Index: %d]\n",
        rf.id, args.LeaderID, len(args.Entries), args.PrevLogTerm, args.PrevLogIndex)

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

    if ok, conflictIndex, conflictTerm := rf.conflictLogAt(args.PrevLogIndex, args.PrevLogTerm); ok {
        DPrintf("AppendEntries: server <%s> conflict with server <%s>'s log at index [%d]\n",
            args.LeaderID, rf.id, args.PrevLogIndex)
        reply.ConflictLogIndex = conflictIndex
        reply.ConflictLogTerm = conflictTerm
        return
    }

    // check entry confliction
    i := 0
    lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
    for ; i < len(args.Entries); i++ {
        if len(rf.log) <= i + args.PrevLogIndex {
            break
        }
        if rf.log[i + args.PrevLogIndex].Term != args.Entries[i].Term {
            rf.log = rf.log[:i + args.PrevLogIndex]
            break
        }
    }
    args.Entries = args.Entries[i:]

    // append new entries
    rf.log = append(rf.log, args.Entries...)
    if (len(args.Entries) > 0) {
        DPrintf("AppendEntries: server <%s> appending [%d] entries from server <%s>\n",
            rf.id, len(args.Entries), args.LeaderID)
    }

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, lastNewEntryIndex)
    }

    reply.Success = true

    rf.persist()
}

// return (isConflict, conflictLogIndex, conflictLogTerm)
func (rf *Raft) conflictLogAt(index, term int) (bool, int, int) {
    if len(rf.log) < index {
        return true, len(rf.log), 0
    }
    // TODO
    if index == 0 || rf.log[index - 1].Term == term {
        // return term == 0
        return false, 0, 0
    }
    i := index
    for ; i >= 1; i-- {
        if rf.log[i - 1].Term != rf.log[index - 1].Term {
            break
        }
    }
    return true, i + 1, rf.log[index - 1].Term
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
        DPrintf("server <%s> granting vote to server <%s>, LastLogTerm=[%d], LastLogIndex=[%d], lastLogIndex=[%d], lastLogTerm=[%d]\n",
            rf.id, args.CandidateID, args.LastLogTerm, args.LastLogIndex, len(rf.log),
            func() int {
                if len(rf.log) > 0 {
                    return rf.log[len(rf.log) - 1].Term
                }
                return 0
            }())
        rf.votedFor = args.CandidateID
        reply.VoteGranted = true
        rf.lastHeartbeat = time.Now()
    }

    rf.persist()
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
    // Your code here (2B).
    term, isLeader := rf.GetState()

    if !isLeader {
        return -1, term, isLeader
    }

    rf.Lock()
    defer rf.Unlock()

    rf.log = append(rf.log, LogEntry{
        Term: rf.currentTerm,
        Command: command,
    })
    index := len(rf.log)

    DPrintf("server <%s> start cmd [%v] at index=[%d]\n",
        rf.id, command, index)

    rf.persist()

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

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go rf.startElectionProcess()
    go rf.startApplyProcess(applyCh)

    return rf
}

