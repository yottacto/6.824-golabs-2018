package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func SendRPCRequest(requestName string, request func() bool) bool {
    for {
        ok := request()
        if ok {
            return true
        }
    }
    return false
}

