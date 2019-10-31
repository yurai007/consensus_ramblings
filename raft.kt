import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.math.*
import kotlin.collections.mutableListOf

// ref: https://raft.github.io/raft.pdf

enum class State {
    INITIAL, DONE
}

interface Message
data class HeartBeat(val done : Boolean = false) : Message
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
        state = State.INITIAL
        currentTerm++
        commitIndex = max(log.size - 1, 0)

        while (!done(receiveHeartbeat())) {

            val appendEntries = receiveAppendEntriesReq()
            var apply = true
            // [1]
            if (appendEntries.term < currentTerm) {
                apply = false
            }
            val prevIndex = appendEntries.prevIndex
            val prevTerm = appendEntries.prevTerm
            if (prevIndex >= 0) {
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

    fun verifyLog(expectedLog : MutableList<InternalLogEntry> ) : Boolean = expectedLog == log

    suspend fun sendHeartbeat(done : Boolean) = channelToLeader.send(HeartBeat(done))
    suspend fun sendAppendEntriesReq(entriesReq : AppendEntriesReq) = channelToLeader.send(entriesReq)
    private suspend fun sendAppendEntriesResp(entriesResp : AppendEntriesResp) = channelToLeader.send(entriesResp)
    private suspend fun receiveAppendEntriesReq() : AppendEntriesReq = channelToLeader.receive() as AppendEntriesReq
    suspend fun receiveAppendEntriesResp() : AppendEntriesResp = channelToLeader.receive() as AppendEntriesResp
    private suspend fun receiveHeartbeat() : HeartBeat = channelToLeader.receive() as HeartBeat
    private fun done(heartBeat : HeartBeat) : Boolean = heartBeat.done

    private val channelToLeader = Channel<Message>()
}

class Leader(followers : List<Follower>, entriesToReplicate : HashMap<Char, Int>) : Node() {

    override suspend fun run() {
        state = State.INITIAL
        currentTerm++
        println("Leader of term $currentTerm")
        entriesToReplicate.forEach { (id, value) ->
            followers.forEach {it.sendHeartbeat(false)}
            val entry = Pair(id, value)
            lastApplied++
            log.add(InternalLogEntry(entry, currentTerm))

            followers.forEach {
                val prevIndex = min(replicaNextIndex(it) - 1, log.size - 1)
                val prevTerm = if (prevIndex >= 0) log[prevIndex].term  else 0
                do {
                    var mayBeCommited = true
                    it.sendAppendEntriesReq(AppendEntriesReq(entry, currentTerm, prevIndex, prevTerm, commitIndex))
                    val response = it.receiveAppendEntriesResp()
                    val expected = AppendEntriesResp(currentTerm, true)
                    if (response != expected) {
                        mayBeCommited = false
                        nextIndex[it] = replicaNextIndex(it) - 1
                        println("No consensus for $id")
                        trackLog()
                    }
                } while (!mayBeCommited)
            }
            commitIndex++
            logState[id] = value
            followers.forEach { nextIndex[it] = replicaNextIndex(it) + 1 }
            println("Leader: $id := $value")
        }
        followers.forEach {it.sendHeartbeat(true)}
        state = State.DONE
        println("Leader: done")
    }

    fun replicateEntries(entriesToReplicate_ : HashMap<Char, Int>) {
        entriesToReplicate = entriesToReplicate_
    }

    private fun replicaNextIndex(replica : Follower) : Int = nextIndex[replica]!!
    private val followers = followers
    private val nextIndex = HashMap<Follower, Int>()
    private val matchIndex = HashMap<Follower, Int>()
    private var entriesToReplicate = entriesToReplicate
    init {
        this.followers.forEach { nextIndex[it] = 0 }
    }
}

class ArtificialFollower : Follower() {
    fun poison(term : Int, log : MutableList<InternalLogEntry>) {
        super.currentTerm = term
        super.log = log
        super.logState.clear()
        super.log.forEach { (entry, _) -> super.logState[entry.first] = entry.second }
    }
}

suspend fun launchLeaderAndFollowers(leader : Leader, followers : List<Follower>) = runBlocking {
    launch {
        try {
            leader.run()
        } catch (e : Exception) { println("Leader: failed") }
    }
    followers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) { println("Follower: failed") }
    } }
}

fun oneLeaderOneFollowerScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('x' to 1, 1),
        InternalLogEntry('y' to 2, 1) ))) }
    println()
}

fun oneLeaderOneFollowerMoreEntriesScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower())
    val entriesToReplicate = hashMapOf('1' to 1, '2' to 2, '3' to 3, '4' to 2, '5' to 1, '6' to 3)
    val leader = Leader(followers, entriesToReplicate)
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('1' to 1, 1),
        InternalLogEntry('2' to 2, 1), InternalLogEntry('3' to 3, 1), InternalLogEntry('4' to 2, 1),
        InternalLogEntry('5' to 1, 1), InternalLogEntry('6' to 3, 1)))) }
    println()
}

fun oneLeaderManyFollowersScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), Follower(), Follower(),
        Follower(), Follower(), Follower(), Follower(), Follower(), Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('x' to 1, 1),
        InternalLogEntry('y' to 2, 1) ))) }
    println()
}

fun oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus() = runBlocking {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), ArtificialFollower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('x' to 1, 1),
        InternalLogEntry('y' to 2, 1) ))) }
    println()
}

fun oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus")
    val entries1 = hashMapOf('a' to 1, 'b' to 2)
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    val artificialFollower = ArtificialFollower()
    val followers = listOf(artificialFollower)

    val leader = Leader(followers, entries1)
    println("Term 1 - replicate entries1")
    launchLeaderAndFollowers(leader, followers)

    leader.replicateEntries(entries2)
    println("Term 2 - replicate entries2")
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('a' to 1, 1),
        InternalLogEntry('b' to 2, 1), InternalLogEntry('d' to 4, 2), InternalLogEntry('c' to 3, 2) ))) }
    println()
}

fun oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus2() = runBlocking {
    println("oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus2")
    val entries1 = hashMapOf('a' to 1, 'b' to 2)
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    val entries3 = hashMapOf('e' to 5)
    val artificialFollower = ArtificialFollower()
    val followers = listOf(artificialFollower)

    val leader = Leader(followers, entries1)
    println("Term 1 - replicate entries1")
    launchLeaderAndFollowers(leader, followers)

    leader.replicateEntries(entries2)
    println("Term 2 - replicate entries2")
    launchLeaderAndFollowers(leader, followers)

    artificialFollower.poison(1, mutableListOf(InternalLogEntry('a' to 1, 1)))
    leader.replicateEntries(entries3)
    println("Term 3 - replicate entries3; follower log is going to be aligned")
    launchLeaderAndFollowers(leader, followers)
    followers.forEach {assert(it.verifyLog(mutableListOf(InternalLogEntry('a' to 1, 1),
        InternalLogEntry('b' to 2, 1), InternalLogEntry('d' to 4, 2), InternalLogEntry('c' to 3, 2),
        InternalLogEntry('e' to 5, 3)))) }
    println()
}

fun main() {
    oneLeaderOneFollowerScenarioWithConsensus()
    oneLeaderOneFollowerMoreEntriesScenarioWithConsensus()
    oneLeaderManyFollowersScenarioWithConsensus()
    oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus()
    oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus()
    oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus2()
}
