package raft

type LogEntry struct {
    // TODO maybe don't need all below
    Term   int
    // Index  int
    // Leader int
    Command    interface{}
}

// Introduced since 2A
type AppendEntriesArgs struct {
    Term         int
    LeaderID     string
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term             int
    Success          bool
    ConflictLogTerm  int
    ConflictLogIndex int
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term         int
    CandidateID  string
    LastLogTerm  int
    LastLogIndex int
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

func (reply *RequestVoteReply) VoteCount() int {
    if reply.VoteGranted {
        return 1
    }
    return 0
}

