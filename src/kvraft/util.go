package raftkv

import (
    "time"
)

// Debugging
const RPCMaxAttempts = 4
const RPCTimeout = 500000 * time.Millisecond

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

