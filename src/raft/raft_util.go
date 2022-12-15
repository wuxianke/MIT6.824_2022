package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) changeState(howtochange int, resetTime bool) {
	if howtochange == TO_FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.getVoteNum = 0
		if resetTime {
			rf.lastResetElectionTime = time.Now()
		}
	}

	if howtochange == TO_CANDIDATE {
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.getVoteNum = 1
		rf.currentTerm += 1

		rf.lastResetElectionTime = time.Now()
	}

	if howtochange == TO_LEADER {
		rf.state = LEADER
		rf.votedFor = -1
		rf.getVoteNum = 0

		rf.nextIndex = make([]int, len(rf.peers))
		// for i := 0; i < len(rf.peers); i++ {
		// 	rf.nextIndex[i] = rf.get
		// }

		rf.lastResetElectionTime = time.Now()
	}
}

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
}

func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1 + rf.lastSSPointIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	// TODO fix it in lab4
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTermWithIndex(newEntryBeginIndex)
}

func (rf *Raft) getLogWithIndex(globalIndex int) Entry {

	return rf.log[globalIndex-rf.lastSSPointIndex]
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	//log.Printf("[GetLogTermWithIndex] Sever %d,lastSSPindex %d ,len %d",rf.me,rf.lastSSPointIndex,len(rf.log))
	if globalIndex-rf.lastSSPointIndex == 0 {
		return rf.lastSSPointTerm
	}
	return rf.log[globalIndex-rf.lastSSPointIndex].Term
}

func (rf *Raft) printLogsForDebug() {
	DPrintf("[PrintLog]Print server %d Logs, lastSSPindex %d", rf.me, rf.lastSSPointIndex)
	for index := 1; index < len(rf.log); index++ {
		DPrintf("[Logs...]Index %d, command %v, term %d", index+rf.lastSSPointIndex, rf.log[index].Command, rf.log[index].Term)
	}

}
