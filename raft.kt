import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.system.measureTimeMillis

// ref: https://raft.github.io/raft.pdf

enum class State {
    INITIAL
}

interface Message
data class HeartBeat(val dummy : Boolean = true) : Message
data class AppendEntriesReq(val entry : Pair<Char, Int>, val term : Int, val prevIndex : Int, val prevTerm : Int, val leaderCommit : Int) : Message
data class AppendEntriesResp(val term : Int, val success : Boolean) : Message
data class InternalLogEntry(val entry : Pair<Char, Int>, val term : Int)

abstract class Node {
    abstract suspend fun run()
    val logState = HashMap<Char, Int>()
    val log = mutableListOf<InternalLogEntry>()
    var state: State = State.INITIAL
    var currentTerm = 0
    var commitIndex = 0
    var lastApplied = 0
}

class Follower() : Node() {

    override suspend fun run() {
        assert(state == State.INITIAL)
        currentTerm++
        val heartbeat = receiveHeartbeat()
        val appendEntries = receiveAppendEntriesReq()
        // [1]
        if (appendEntries.term < currentTerm) {
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, false))
        }
        val index = appendEntries.prevIndex
        if (index < log.size) {
            // [2]
            if (log.get(index).term != appendEntries.prevTerm) {
                sendAppendEntriesResp(AppendEntriesResp(currentTerm, false))
            }
        }
        if (index + 1 < log.size) {
            // [3]
            if (log.get(index + 1).term != appendEntries.term) {
                // TODO: delete the existing entry and all that follow it
            }
        }
        val (id, value) = appendEntries.entry
        if (false) {
            commitIndex++
            logState.set(id, value)
            println("Follower: $id := $value")
        } else {
            println("No consensus for $id")
        }
        state = State.INITIAL
    }

    suspend fun sendHeartbeat() = channelToLeader.send(HeartBeat())
    suspend fun sendAppendEntriesReq(entriesReq : AppendEntriesReq) = channelToLeader.send(entriesReq)
    suspend fun sendAppendEntriesResp(entriesResp : AppendEntriesResp) = channelToLeader.send(entriesResp)
    suspend fun receiveAppendEntriesReq() : AppendEntriesReq = channelToLeader.receive() as AppendEntriesReq
    suspend fun receiveAppendEntriesResp() : AppendEntriesResp = channelToLeader.receive() as AppendEntriesResp
    suspend fun receiveHeartbeat() : Message = channelToLeader.receive()

    val channelToLeader = Channel<Message>()
}

class Leader(followers : List<Follower>, entriesToReplicate : HashMap<Char, Int>) : Node() {
    override suspend fun run() {
        assert(state == State.INITIAL)
        currentTerm++
        followers.forEach {it.sendHeartbeat()}
        entriesToReplicate.forEach { (id, value) ->
                val entry = Pair<Char, Int>(id, value)
                var prevIndex = 0
                var prevTerm = 0
                if (!log.isEmpty()) {
                    prevIndex = log.size - 1
                    val lastEntry = log.get(prevIndex)
                    prevTerm = lastEntry.term
                }
                log.add(InternalLogEntry(entry, currentTerm))
                followers.forEach {it.sendAppendEntriesReq(AppendEntriesReq(entry, currentTerm, prevIndex, prevTerm, commitIndex))}
                val responses = mutableListOf<AppendEntriesResp>()
                followers.forEach {responses.add(it.receiveAppendEntriesResp())}
                if (false) {
                    commitIndex++
                    logState.set(id, value)
                    println("Leader: $id := $value")
                } else {
                    println("No consensus for $id")
                }
        }
        state = State.INITIAL
    }

    val followers = followers
    val nextIndex = HashMap<Follower, Int>()
    val matchIndex = HashMap<Follower, Int>()
    val entriesToReplicate = entriesToReplicate
}

fun oneLeaderOneFollowerScenarioWithConsensus() = runBlocking<Unit> {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch { leader.run() }
    followers.forEach { launch { it.run() } }
}

fun oneLeaderOneFollowerMoreEntriesScenarioWithConsensus() = runBlocking<Unit> {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2, 'x' to 3, 'z' to 2, 'y' to 1, 'y' to 3)
    val leader = Leader(followers, entriesToReplicate)
    launch { leader.run() }
    followers.forEach { launch { it.run() } }
}

fun oneLeaderManyFollowersScenarioWithConsensus() = runBlocking<Unit> {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), Follower(), Follower(),
                 Follower(), Follower(), Follower(), Follower(), Follower(), Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch { leader.run() }
    followers.forEach { launch { it.run() } }
}

fun main(args: Array<String>) {
    oneLeaderOneFollowerScenarioWithConsensus()
    oneLeaderOneFollowerMoreEntriesScenarioWithConsensus()
    oneLeaderManyFollowersScenarioWithConsensus()
}
