import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.system.measureTimeMillis
import kotlin.math.*

// ref: https://raft.github.io/raft.pdf

enum class State {
    INITIAL, DONE
}

interface Message
data class HeartBeat(val dummy : Boolean = true) : Message
data class AppendEntriesReq(val entry : Pair<Char, Int>, val term : Int, val prevIndex : Int, val prevTerm : Int, val leaderCommit : Int) : Message
data class AppendEntriesResp(val term : Int, val success : Boolean) : Message
data class InternalLogEntry(val entry : Pair<Char, Int>, val term : Int)

abstract class Node {
    abstract suspend fun run()
    val logState = HashMap<Char, Int>()
    var log = mutableListOf<InternalLogEntry>()
    var state: State = State.INITIAL
    var currentTerm = 0
    var commitIndex = 0
    var lastApplied = 0
    private val debug = true

    fun trackLog() {
        if (debug) {
            println("$log")
        }
    }
}

open class Follower : Node() {

    override suspend fun run() {
        assert(state == State.INITIAL)
        currentTerm++
        commitIndex = max(log.size - 1, 0)

        while (!done()) {
            receiveHeartbeat()
            val appendEntries = receiveAppendEntriesReq()
            var apply = true
            // [1]
            if (appendEntries.term < currentTerm) {
                apply = false
            }
            val prevIndex = appendEntries.prevIndex
            val prevTerm = appendEntries.prevTerm
            if (prevIndex < log.size) {
                // [2]
                if (log[prevIndex].term != prevTerm) {
                    apply = false
                }
            }
            if (prevIndex + 1 < log.size) {
                // [3]
                if (log[prevIndex + 1].term != appendEntries.term) {
                    // cleanup because of inconsistency, leave only log[0..prevIndex] prefix,
                    // in next iteration in case of further inconsistency we should reach [2]
                    shrinkUntil(prevIndex)
                    apply = false
                }
            }
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, apply))
            val (id, value) = appendEntries.entry
            if (apply) {
                log.add(InternalLogEntry(appendEntries.entry, currentTerm))
                logState[id] = value
                println("Follower $this: $id := $value")
                commitIndex++
                lastApplied++
            } else {
                println("Follower $this: no consensus for $id")
                trackLog()
            }
        }
        state = State.DONE
        println("Follower $this: done")
    }

    private fun shrinkUntil(index : Int) {
        for (i in index + 1..log.size-1) {
            val (keyValue, _) = log[i]
            val (key, _) = keyValue
            logState.remove(key)
            log.removeAt(i)
        }
    }

    suspend fun sendHeartbeat() = channelToLeader.send(HeartBeat())
    suspend fun sendAppendEntriesReq(entriesReq : AppendEntriesReq) = channelToLeader.send(entriesReq)
    private suspend fun sendAppendEntriesResp(entriesResp : AppendEntriesResp) = channelToLeader.send(entriesResp)
    private suspend fun receiveAppendEntriesReq() : AppendEntriesReq = channelToLeader.receive() as AppendEntriesReq
    suspend fun receiveAppendEntriesResp() : AppendEntriesResp = channelToLeader.receive() as AppendEntriesResp
    private suspend fun receiveHeartbeat() : Message = channelToLeader.receive()
    private fun done() : Boolean = channelToLeader.isClosedForReceive
    fun close() : Boolean = channelToLeader.close()

    private val channelToLeader = Channel<Message>()
}

class Leader(followers : List<Follower>, entriesToReplicate : HashMap<Char, Int>) : Node() {
    override suspend fun run() {
        assert(state == State.INITIAL)
        currentTerm++
        followers.forEach { nextIndex[it] = 0 }
        entriesToReplicate.forEach { (id, value) ->
            followers.forEach {it.sendHeartbeat()}
            val entry = Pair(id, value)
            lastApplied++
            log.add(InternalLogEntry(entry, currentTerm))

            followers.forEach {
                val prevIndex = min(nextIndex[it]!!, log.size - 1)
                val lastEntry = log[prevIndex]
                val prevTerm = lastEntry.term
                do {
                    var mayBeCommited = true
                    it.sendAppendEntriesReq(AppendEntriesReq(entry, currentTerm, prevIndex, prevTerm, commitIndex))
                    val response = it.receiveAppendEntriesResp()
                    val expected = AppendEntriesResp(currentTerm, true)
                    if (response != expected) {
                        mayBeCommited = false
                        nextIndex[it] = nextIndex[it]!! - 1
                        println("No consensus for $id")
                        trackLog()
                    }
                } while (!mayBeCommited)
            }
            commitIndex++
            logState[id] = value
            followers.forEach { nextIndex[it] = nextIndex[it]!! + 1 }
            println("Leader: $id := $value")
        }
        followers.forEach { it.close() }
        state = State.DONE
        println("Leader: done")
    }

    private val followers = followers
    private var nextIndex = HashMap<Follower, Int>()
    private val matchIndex = HashMap<Follower, Int>()
    private val entriesToReplicate = entriesToReplicate
}

class ArtificialFollower() : Follower() {
    constructor(log : MutableList<InternalLogEntry>) : this() {
        super.log = log
    }
}

fun oneLeaderOneFollowerScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch {
        try {
            leader.run()
        } catch (e : Exception) {}
    }
    followers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) {}
    } }
}

fun oneLeaderOneFollowerMoreEntriesScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2, 'x' to 3, 'z' to 2, 'y' to 1, 'y' to 3)
    val leader = Leader(followers, entriesToReplicate)
    launch {
        try {
            leader.run()
        } catch (e : Exception) {}
    }
    followers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) {}
    } }
}

fun oneLeaderManyFollowersScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), Follower(), Follower(),
        Follower(), Follower(), Follower(), Follower(), Follower(), Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch {
        try {
            leader.run()
        } catch (e : Exception) {}
    }
    followers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) {}
    } }
}

fun oneLeaderManyFollowersWithArtificalOneScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), ArtificialFollower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch {
        try {
            leader.run()
        } catch (e : Exception) {}
    }
    followers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) {}
    } }
}

fun main() {
    oneLeaderOneFollowerScenarioWithConsensus()
    oneLeaderOneFollowerMoreEntriesScenarioWithConsensus()
    oneLeaderManyFollowersScenarioWithConsensus()
    oneLeaderManyFollowersWithArtificalOneScenarioWithConsensus()
}
