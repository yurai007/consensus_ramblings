import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.system.measureTimeMillis

// ref: https://raft.github.io/raft.pdf

enum class State {
    INITIAL
}

interface Message
data class HeartBeat(val dummy : Boolean = true) : Message
data class AppendEntriesReq(val entry : Pair<Char, Int>, val term : Int, val prevIndex : Int, val prevTerm : Int) : Message
data class AppendEntriesResp(val term : Int, val success : Boolean) : Message
data class InternalLogEntry(val entry : Pair<Char, Int>, val term : Int)

abstract class Node {
    abstract suspend fun run()
    val logState = HashMap<Char, Int>()
    val log = mutableListOf<InternalLogEntry>()
    var state: State = State.INITIAL
    var currentTerm = 0
}

class Follower() : Node() {

    override suspend fun run() {
        assert(state == State.INITIAL)
        val heartbeat = receiveHeartbeat()
        val appendEntries = receiveAppendEntriesReq()
        if (appendEntries.term < currentTerm) {
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, false))
        }
        val (id, value) = appendEntries.entry
        if (false) {
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
        followers.forEach {it.sendHeartbeat()}
        entriesToReplicate.forEach { (id, value) ->
                currentTerm++
                logState.set(id, value)
                val entry = Pair<Char, Int>(id, value)
                log.add(InternalLogEntry(entry, currentTerm))
                followers.forEach {it.sendAppendEntriesReq(AppendEntriesReq(entry, currentTerm, 0, 0))}
                val responses = mutableListOf<AppendEntriesResp>()
                followers.forEach {responses.add(it.receiveAppendEntriesResp())}
                if (false) {
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
    var commitIndex = 0
}

fun oneLeaderOneFollowerScenarioWithConsensus() = runBlocking<Unit> {
    val followers = listOf(Follower(), Follower(), Follower(), Follower(), Follower(), Follower(), Follower(),
                 Follower(), Follower(), Follower(), Follower(), Follower(), Follower())
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val leader = Leader(followers, entriesToReplicate)
    launch { leader.run() }
    followers.forEach { launch { it.run() } }
}

fun main(args: Array<String>) {
    oneLeaderOneFollowerScenarioWithConsensus()
}
