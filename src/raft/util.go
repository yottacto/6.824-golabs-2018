package raft

import (
    "log"
    "time"
)

// Debugging
const Debug = 0
const RPCMaxAttempts = 4
const RPCTimeout = 50 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func Max(x, y int) int {
    if x > y {
        return x
    }
    return y
}

func SendRPCRequest(requestName string, request func() bool) bool {
    DPrintf("start request: %s", requestName)
    makeRequest := func(success chan bool) {
        if request() {
            success <- true
        }
    }
    for i := 0; i < RPCMaxAttempts; i++ {
        success := make(chan bool, 1)
        go makeRequest(success)
        select {
            case <-success:
                return true
            case <-time.After(RPCTimeout):
                DPrintf("request %s attempt [%d] timeout", requestName, i)
                continue
        }
    }
    DPrintf("rpc %s failed", requestName)
    return false
}

