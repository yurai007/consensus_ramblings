import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.system.measureTimeMillis

enum class State {
    INITIAL
}

interface Message
data class HeartBeat(val dummy : Boolean = true) : Message
data class AppendEntries(val entry : Pair<Int, Int>, val term : Int, prevIndex : Int, prevTerm : Int) : Message

data class InternalLogEntry(val entry : Pair<Char, Int>, val term : Int)

abstract class Node {
    abstract suspend fun run()
    val logState = HashMap<Char, Int>()
    val log = mutableListOf<InternalLogEntry>()
    var state: State = State.INITIAL
}

class Follower() : Node() {

    override suspend fun run() {
        assert(state == State.INITIAL)

        state = State.INITIAL
    }

    suspend fun sendHeartbeat() = channelToLeader.send(HeartBeat())
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
                log.add(InternalLogEntry(Pair<Char, Int>(id, value), currentTerm))
        }
        state = State.INITIAL
    }

    val followers = followers
    val entriesToReplicate = entriesToReplicate
    var currentTerm = 0
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
