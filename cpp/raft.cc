#include "cooperative_scheduler.hh"
#include "channel.hh"
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <fmt/core.h>
#include <tuple>
#include <map>
#include <vector>
#include <cassert>
#include <future>
#include <stdexcept>
#include <compare>
#include <memory>
#include <random>

// ref: https://raft.github.io/raft.pdf

enum class State {
    FOLLOWER, CANDIDATE, LEADER, DONE
};

struct Message {
    virtual ~Message() = default;
};

// std::make_unique require message constructors
struct HeartBeat final : Message {
    HeartBeat(bool _done) noexcept : done(_done) {}
    bool done = false;
};

struct MetaEntry {
    MetaEntry(const std::tuple<char, int> &_entry, int _term) noexcept
        : entry(_entry), term(_term) {}
    std::tuple<char, int> entry;
    int term;
    bool operator==(const MetaEntry& rhs) const noexcept {
        auto first = term == rhs.term;
        return first? (entry == rhs.entry) : false;
    }
};

struct AppendEntriesReq final : Message {
    AppendEntriesReq(const MetaEntry &_metaEntry, int _term, int _prevIndex, int _prevTerm, int _leaderCommit) noexcept
        : metaEntry(_metaEntry), term(_term), prevIndex(_prevIndex), prevTerm(_prevTerm), leaderCommit(_leaderCommit) {}

    MetaEntry metaEntry;
    int term, prevIndex, prevTerm, leaderCommit;
};

struct AppendEntriesResp final : Message {
    AppendEntriesResp(int _term, bool _success) noexcept
        : term(_term), success(_success) {}
    int term;
    bool success;
    // a little bit more general but still cool
    auto operator<=>(const AppendEntriesResp& rhs) const noexcept {
        auto first = term <=> rhs.term;
        return (first != 0)? first : success <=> rhs.success;
    }
};

struct RequestVoteReq final : Message {
    RequestVoteReq(int _term, void *_candidateId, int _lastLogIndex, int _lastLogTerm) noexcept
        : term(_term), candidateId(_candidateId), lastLogIndex(_lastLogIndex), lastLogTerm(_lastLogTerm) {}
    int term;
    void *candidateId;
    int lastLogIndex, lastLogTerm;
};

struct RequestVoteResp final : Message {
    RequestVoteResp(int _term, bool _voteGranted) noexcept
        : term(_term), voteGranted(_voteGranted) {}
    int term;
    bool voteGranted;
};

static void printLog(const std::vector<MetaEntry> &log) {
    for (auto &&item : log) {
        fmt::print("({}, {}) {}  ", std::get<0>(item.entry), std::get<1>(item.entry), item.term);
    }
    fmt::print("\n");
}

static auto nextInt() {
    static int i = 0;
    return ++i;
}

class Node {
public:
    Node() = default;
    Node(const std::map<char, int> &_logState, const std::vector<MetaEntry> &_log, State _state)
        : logState(_logState), log(_log), state(_state) {}
    virtual void run() = 0;
    virtual ~Node() = default;
    virtual Node *me() {
        return this;
    }

    void trackLog() const {
        if (debug) {
            printLog(log);
        }
    }

    bool verifyLog(const std::vector<MetaEntry> &expectedLog) const {
        return expectedLog == log;
    }

protected:
    std::map<char, int> logState;
    std::vector<MetaEntry> log;
public:
    State state = State::FOLLOWER;
    int currentTerm = 0, commitIndex = 0, lastApplied = 0;
    constexpr static bool debug = true;
    constexpr static int rpcTimeoutMs = 60;
};

class Follower : public Node {
public:
    Follower() = default;
    Follower(const std::map<char, int> &_logState, const std::vector<MetaEntry> &_log, State _state, bool _delayed)
        : Node(_logState, _log, _state), delayed(_delayed) {}

    void run() override {
        const void *me = this;
        fmt::print("Follower {}: starts\n", me);
        state = State::FOLLOWER;
        currentTerm++;
        const auto logSize = static_cast<int>(log.size());
        commitIndex = std::max(logSize, 0);

        auto maybeHeartBeat = receiveHeartbeat().get();
        while (maybeHeartBeat != nullptr && !done(*maybeHeartBeat)) {
            auto maybeAppendEntries = receiveAppendEntriesReq().get();
            if (maybeAppendEntries == nullptr) {
                break;
            }
            auto &appendEntries = *maybeAppendEntries;
            auto apply = true;
            auto prevIndex = appendEntries.prevIndex;
            auto prevTerm = appendEntries.prevTerm;
            // [1]
            if (appendEntries.term < currentTerm) {
                apply = false;
            } else {
                // [1]
                if (appendEntries.term > currentTerm) {
                   currentTerm = appendEntries.term;
                }
                if (prevIndex >= 0) {
                    if (prevIndex < log.size()) { // WTF, logSize is wrong (0) but log.size() ok?
                        // [2]
                        if (log[prevIndex].term != prevTerm) {
                            apply = false;
                        }
                    } else {
                        apply = false;
                    }
                    if (prevIndex + 1 < log.size()) {
                        // [3]
                        if (log[prevIndex + 1].term != appendEntries.metaEntry.term) {
                            // cleanup because of inconsistency, leave only log[0..prevIndex] prefix,
                            // with assumption that prefix is valid we can append entry in this iteration
                            shrinkUntil(prevIndex);
                        }
                    }
                }
            }
            auto appendEntries_cp = std::make_unique<AppendEntriesReq>(appendEntries);
            // at this point previous msg from channel ends lifetime so we need copy it
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, apply)).get();
            auto metaEntry = appendEntries_cp->metaEntry;
            auto leaderCommit = appendEntries_cp->leaderCommit;
            auto [id, value] = metaEntry.entry;
            if (apply) {
                // [4]
                log.emplace_back(metaEntry.entry, metaEntry.term);
                logState[id] = value;
                fmt::print("Follower {}: {} := {}\n", me, id, value);
                lastApplied++;
                // [5]
                commitIndex = std::min(leaderCommit + 1, commitIndex + 1);
            } else {
                fmt::print("Follower {}: no consensus for {}\n", me, id);
                trackLog();
            }
            maybeHeartBeat = receiveHeartbeat().get();
        }
        if (maybeHeartBeat != nullptr && maybeHeartBeat->done) {
            state = State::DONE;
            fmt::print("Follower {}: done with commitIndex = {}\n", me, commitIndex);
            trackLog();
        } else {
            state = State::CANDIDATE;
            fmt::print("Follower {}: Heartbeat or AppendEntriesReq failed with election timeout. Start election\n", me);
        }
    }
    virtual ~Follower() = default;
private:
#ifndef __clang__
    static_assert(CoroutineChannel<channel, Message*>, "it's important for implementation to have fully preemptable "
                                                       "coroutine channel");
#endif
    channel<Message*> channelToLeader;
    std::unique_ptr<Message> msg;
    Message *msg_p = nullptr;
    bool delayed = false;

   void shrinkUntil(int index) {
        for (auto i : boost::irange(index + 1, static_cast<int>(log.size()))) {
            auto &&[keyValue, _] = log[i];
            auto &&[key, __] = keyValue;
            logState.erase(key);
        }
        log.erase(log.begin() + index + 1, log.end());
    }

    bool done(const HeartBeat &_msg) const noexcept {
        return _msg.done;
    }
public:
    std::future<bool> sendHeartbeat(bool done) {
       if (!delayed) {
           msg = std::make_unique<HeartBeat>(done);
           msg_p = msg.get();
           co_await channelToLeader.write(msg_p);
           co_return true;
       } else {
           fmt::print("Leader: delay\n");
           co_await delay(2*rpcTimeoutMs);
           co_return false;
       }
    }

    std::future<HeartBeat*> receiveHeartbeat() {
        const void *me = this;
        if (!delayed) {
            auto [_msg, ok] = co_await channelToLeader.read();
            co_return dynamic_cast<HeartBeat*>(_msg);
        } else {
            fmt::print("Follower {}: delay\n", me);
            co_await delay(rpcTimeoutMs);
            co_return nullptr;
        }
    }

    std::future<bool> sendAppendEntriesReq(AppendEntriesReq &&req) {
        msg = std::make_unique<AppendEntriesReq>(req);
        msg_p = msg.get();
        co_await channelToLeader.write(msg_p);
        co_return true;
    }

    std::future<AppendEntriesReq*> receiveAppendEntriesReq() {
        auto [_msg, ok] = co_await channelToLeader.read();
        co_return dynamic_cast<AppendEntriesReq*>(_msg);
    }

    std::future<bool> sendAppendEntriesResp(AppendEntriesResp &&rep) {
        msg = std::make_unique<AppendEntriesResp>(rep);
        msg_p = msg.get();
        co_await channelToLeader.write(msg_p);
        co_return true;
    }

    std::future<AppendEntriesResp*> receiveAppendEntriesResp() {
        auto [_msg, ok] = co_await channelToLeader.read();
        co_return dynamic_cast<AppendEntriesResp*>(_msg);
    }
};

class Leader final : public Node {
public:
    Leader(std::vector<Follower*> _followers, const std::map<char, int> &_entriesToReplicate,
           const std::map<char, int> &_logState, const std::vector<MetaEntry> &_log, State _state) :
        Node(_logState, _log, _state),
        followers(_followers),
        entriesToReplicate(_entriesToReplicate) {
        boost::for_each(followers, [this](auto &&follower){
            nextIndex[follower] = 0;
            matchIndex[follower] = -1;
        });
    }

    void run() override {
        state = State::LEADER;
        currentTerm++;
        fmt::print("Leader of term {}\n", currentTerm);
        for (auto &&entry : entriesToReplicate) {
            for (auto &&follower : followers) {
                // FIXME: do in parallel + when_all helper
                if (!follower->sendHeartbeat(false).get()) {
                    // FIXME: fallback here and there is not the proper way to handle slow Follower
                    fallbackTo(State::CANDIDATE);
                    return; // different than lambda return!
                }
            }
            lastApplied++;
            // [5.3]
            log.emplace_back(entry, currentTerm);
            // x = x, as workaround for http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/p0588r1.html
            boost::for_each(followers, [this, metaEntry = MetaEntry{entry, currentTerm}, followerIsDone = false]
                            (auto &&follower) mutable {
                 auto &it = *follower;
                 do {
                    auto [id, value] = metaEntry.entry;
                    const auto logSize = static_cast<int>(log.size());
                    auto prevIndex = std::min(replicaNextIndex(&it) - 1, logSize - 1);
                    auto prevTerm = (prevIndex >= 0)? log[prevIndex].term  : 0;
                    it.sendAppendEntriesReq(AppendEntriesReq(metaEntry, currentTerm, prevIndex, prevTerm,
                            commitIndex)).get();
                    auto maybeResponse = it.receiveAppendEntriesResp().get();
                    if (maybeResponse == nullptr){
                        fmt::print("Leader: No AppendEntriesResp from {}. Should try again later\n", static_cast<void*>(&it));
                        break;
                    }
                    auto response = *maybeResponse;
                    if (auto expected = AppendEntriesResp(currentTerm, true); response <=> expected != 0) {
                        fmt::print("Leader: No consensus for {} {}\n", id, value);
                        if (!response.success && response.term > currentTerm) {
                            // [5.1]
                            fallbackTo(State::FOLLOWER);
                        }
                        // [5.3]
                        nextIndex[&it] = replicaNextIndex(&it) - 1;
                        if (replicaNextIndex(&it) >= 0) {
                            metaEntry = log[replicaNextIndex(&it)];
                        }
                        if (!it.sendHeartbeat(false).get()) {
                            fallbackTo(State::CANDIDATE);
                        }
                        trackLog();
                    } else if (replicaNextIndex(&it) == logSize - 1) {
                        followerIsDone = true;
                    } else {
                        nextIndex[&it] = replicaNextIndex(&it) + 1;
                        if (replicaNextIndex(&it) < logSize) {
                            metaEntry = log[replicaNextIndex(&it)];
                        } else {
                            metaEntry = {std::tie(id, value), currentTerm};
                        }
                        if (!it.sendHeartbeat(false).get()) {
                            fallbackTo(State::CANDIDATE);
                        }
                    }
                 } while (!followerIsDone);
            });
            auto [id, entry_value] = entry;
            logState[id] = entry_value;
            // [5.3]
            boost::for_each(followers, [this](auto &&it){
                matchIndex[it] = replicaNextIndex(it);
                nextIndex[it] = replicaNextIndex(it) + 1;
            });
            // [5.3] [5.4]
            using namespace boost::adaptors;
            auto indexList =  boost::copy_range<std::vector<int>>(matchIndex | map_values);
            boost::range::sort(indexList);
            auto majorityCommitIndex = (log[indexList[indexList.size()/2]].term == currentTerm)?
                        indexList[indexList.size()/2] :0;
            commitIndex = std::max(majorityCommitIndex, commitIndex + 1);
            fmt::print("Leader: {} := {}\n", id, entry_value);
        }
        boost::for_each(followers, [](auto &&follower){
            follower->sendHeartbeat(true).get();
        });
        state = State::DONE;
        fmt::print("Leader: done with commitIndex = {}\n", commitIndex);
    }

    void replicateEntries(const std::map<char, int> &entriesToReplicate_) {
        entriesToReplicate = entriesToReplicate_;
    }

    const std::vector<MetaEntry> &getCurrentLog() const {
        return log;
    }

private:
     std::vector<Follower*> followers;
     std::map<Follower*, int> nextIndex, matchIndex;
     std::map<char, int> entriesToReplicate;

     void fallbackTo(State _state) {
         state = _state;
         fmt::print("Leader: need to fallback to state = {}\n", state);
     }

     int replicaNextIndex(Follower *follower) const {
         auto it = nextIndex.find(follower);
         return (it != nextIndex.end())? it->second : (throw std::logic_error("Shouldn't happen"), 42);
     }
};

class Candidate final : public Node {
public:
    Candidate(int _expectedCandidates, const std::vector<Candidate*> &_otherCandidates, const std::map<char, int> &_logState,
              const std::vector<MetaEntry> &_log, State _state)
        : Node(_logState, _log, _state), expectedCandidates(_expectedCandidates), otherCandidates(_otherCandidates){}

    void run() override {
        state = State::CANDIDATE;
        const void *me = this;
        fmt::print("Candidate {}: start\n", me);
        auto endOfElection = false;
        // only main happy path for now
        while (!endOfElection) {
             currentTerm++;
             if (otherCandidates.size() < expectedCandidates) {
                 fmt::print("Candidate {}: too less candidates. Wait half of election timeout to catch up\n", me);
                 wait(rpcTimeoutMs).get();
                 return;
             }
             auto message = receiveRequestVoteReqOrLeaderMessage().get();
             if (message != nullptr) {
                   auto vote  = true;
                   if (auto requestVoteReq = dynamic_cast<RequestVoteReq*>(message); !requestVoteReq) {
                       auto maybeVoter = boost::find_if(otherCandidates, [&](auto &&it){
                           return it == requestVoteReq->candidateId; });
                       auto voter = *maybeVoter;
                       if (requestVoteReq->term >= currentTerm) {
                           if (requestVoteReq->lastLogIndex < 0) {

                           } else {
                                assert(false); // FIXME - test it
                                if (requestVoteReq->lastLogIndex < log.size()) {
                                    vote = log[requestVoteReq->lastLogIndex].term <= requestVoteReq->lastLogTerm;
                                } else {
                                    vote = false;
                                }
                           }
                       } else {
                           vote = false;
                       }
                       if (vote) {
                           fmt::print("Candidate {}: vote for {} + transition to Follower\n", me, static_cast<void*>(voter));
                           sendRequestVoteResp(*voter, RequestVoteResp(currentTerm, true)).get();
                           endOfElection = true;
                           state = State::FOLLOWER;
                       } else {
                           sendRequestVoteResp(*voter, RequestVoteResp(currentTerm, false)).get();
                       }

                   } else if (typeid(*message) == typeid(AppendEntriesReq) || typeid(*message) == typeid(HeartBeat)) {
                       fmt::print("Candidate {}: received Leader's message. Transition to Follower\n", me);
                       endOfElection = true;
                       state = State::FOLLOWER;
                   }
             } else {
                   auto votesForMe = 0u;
                   boost::for_each(otherCandidates, [this, &votesForMe, &endOfElection, me](auto &&it){
                       auto lastLogIndex = log.size() - 1;
                       auto lastLogTerm = (lastLogIndex >= 0u)? log[lastLogIndex].term :0;
                       auto requestVoteReq = RequestVoteReq(currentTerm, this, lastLogIndex, lastLogTerm);
                       sendRequestVoteReq(*it, std::move(requestVoteReq)).get();
                       auto maybeMessage = receiveRequestVoteRespOrLeaderMessage().get(); //FIXME: this?
                       if (maybeMessage == nullptr) {
                            assert(false);
                       } else {
                           if (auto requestVoteResp = dynamic_cast<RequestVoteResp*>(maybeMessage);
                                   requestVoteResp && requestVoteResp->voteGranted) {
                               votesForMe++;
                           } else if (typeid(*maybeMessage) == typeid(AppendEntriesReq) ||
                                      typeid(*maybeMessage) == typeid(HeartBeat)) {
                               fmt::print("Candidate {}: received Leader's message. Transition to Follower\n", me);
                               endOfElection = true;
                               state = State::FOLLOWER;
                           }
                       }
                   });
                   if (votesForMe > otherCandidates.size()/2) {
                       fmt::print("Candidate {}: become Leader\n", me);
                       state = State::LEADER;
                       endOfElection = true;
                   }
             }
        }
    }

    std::future<bool> wait(unsigned timeout) {
       co_await delay(timeout);
       co_return true;
    }

    void setCandidates(const std::vector<Candidate*> &_otherCandidates) {
        otherCandidates = _otherCandidates;
    }

private:
    int expectedCandidates;
    std::vector<Candidate*> otherCandidates;
    std::unique_ptr<Message> msg;
    Message *msg_p = nullptr;
#ifndef __clang__
    static_assert(CoroutineChannel<channel, Message*>, "it's important for implementation to have fully preemptable "
                                                       "coroutine channel");
#endif
public:
    channel<Message*> _channel;
private:

    std::future<bool> sendRequestVoteReq(Candidate &candidate, RequestVoteReq &&requestVote) {
        msg = std::make_unique<RequestVoteReq>(requestVote);
        msg_p = msg.get();
        co_await candidate._channel.write(msg_p);
        co_return true;
    }

    std::future<Message*> receiveRequestVoteReqOrLeaderMessage() {
        auto timeoutMs = nextInt()*rpcTimeoutMs;
        auto [_msg, ok] = co_await _channel.readWithTimeout(timeoutMs);
        co_return _msg;
    }

    std::future<bool> sendRequestVoteResp(Candidate &candidate, RequestVoteResp &&requestVoteResp) {
        msg = std::make_unique<RequestVoteResp>(requestVoteResp);
        msg_p = msg.get();
        co_await candidate._channel.write(msg_p);
        co_return true;
    }

    std::future<Message*> receiveRequestVoteRespOrLeaderMessage() {
        auto timeoutMs = nextInt()*rpcTimeoutMs;
        auto [_msg, ok] = co_await _channel.readWithTimeout(timeoutMs);
        co_return _msg;
    }
};

class ArtificialFollower : public Follower {
public:
    ArtificialFollower(const std::map<char, int> &_logState, const std::vector<MetaEntry> &_log,
                       State _state, bool _delayed)
        : Follower(_logState, _log, _state, _delayed) {}

    void poison(int term, const std::vector<MetaEntry> &_log) {
        currentTerm = term;
        log = _log;
        logState.clear();
        boost::for_each(log, [this](auto &&entry){
            auto [e1, e2] = entry.entry;
            logState[e1] = e2;
        });
    }
};

enum class Instance {
     FOLLOWER, LEADER, ARTIFICIAL_FOLLOWER
};

class Server final : public Node {
public:
    Server(Instance _startingInstance, std::optional<std::map<char, int>> _maybeEntriesToReplicate,
           std::vector<std::unique_ptr<Node>> &_otherNodes, bool _stopOnStateChangeOnce, bool _delayed = false)
        : maybeEntriesToReplicate(_maybeEntriesToReplicate), otherNodes(_otherNodes),
          stopOnStateChangeOnce(_stopOnStateChangeOnce), delayed(_delayed),
          node(createNode(_startingInstance)) {}

    void run() override {
        while (true) {
            using namespace boost::adaptors;
            auto old_state = node->state;
            node->run();
            state = node->state;
            switch (state) {
                case State::FOLLOWER:
                    fmt::print("transform to follower\n");
                    node = std::make_unique<Follower>(logState, log, state, delayed);
                break;

                case State::LEADER: {
                    auto knownFollowers = boost::copy_range<std::vector<Follower*>>(otherNodes
                        | transformed([](auto &&it){ return dynamic_cast<Follower*>(it->me()); })
                        | filtered([](auto &&it){ return bool(it); }));
                    fmt::print("transform to leader\n");
                    node = std::make_unique<Leader>(knownFollowers, maybeEntriesToReplicate.value(), logState, log, state);
                    break;
                }

                case State::CANDIDATE: {
                    auto knownCandidates = boost::copy_range<std::vector<Candidate*>>(otherNodes
                        | transformed([](auto &&it){ return dynamic_cast<Candidate*>(it->me()); })
                        | filtered([this](auto &&it){ return bool(it) && it != me(); }));
                    auto node_as_candiate = dynamic_cast<Candidate*>(node.get());
                    if (node_as_candiate) {
                        node_as_candiate->setCandidates(knownCandidates);
                    } else {
                        fmt::print("transform to candidate from {}\n", old_state);
                        node = std::make_unique<Candidate>(otherNodes.size() - 1, knownCandidates, logState, log, state);
                    }
                    break;
                }
                case State::DONE: return;
                break;
            }
            if (stopOnStateChangeOnce) {
                stopOnStateChangeOnce = false;
                fmt::print("Stop server on change\n");
                return;
            }
        }
    }

    Node *me() override {
        return node.get();
    }

    std::unique_ptr<Node> createNode(Instance startingInstance) {
        using namespace boost::adaptors;
        switch (startingInstance) {
            case Instance::LEADER: {
                auto knownFollowers = boost::copy_range<std::vector<Follower*>>(otherNodes
                    | transformed([](auto &&it){ return dynamic_cast<Follower*>(it->me()); })
                    | filtered([](auto &&it){ return bool(it); }));
                assert(!knownFollowers.empty());
                return std::make_unique<Leader>(knownFollowers, maybeEntriesToReplicate.value(), logState, log, state);
                break;
            }
            case Instance::FOLLOWER:
                return std::make_unique<Follower>(logState, log, state, delayed);
            break;
            case Instance::ARTIFICIAL_FOLLOWER:
                return std::make_unique<ArtificialFollower>(logState, log, state, delayed);
            break;
        }
        return nullptr;
     }
private:
    std::optional<std::map<char, int>> maybeEntriesToReplicate;
    std::vector<std::unique_ptr<Node>> &otherNodes;
    bool stopOnStateChangeOnce;
    bool delayed;
    std::unique_ptr<Node> node;
};

static void serverFiber(Node *server) {
    auto task = [server]() -> std::future<int> {
         try {
             server->run();
         } catch (...) {  fmt::print("Server {}: failed\n", static_cast<void*>(server)); }
         co_return 0;
    };
    task().get();
}

template<class Fiber>
static void launchServers(Fiber f, const std::vector<std::unique_ptr<Node>> &args) {
    // notice that now leader is running at the end
    cooperative_scheduler{ f, args };
}

static void oneLeaderOneFollowerScenarioWithConsensus() {
    fmt::print("oneLeaderOneFollowerScenarioWithConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, false));
    launchServers(serverFiber, nodes);
    auto expectedLog = {MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1} };
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 2 && node->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerMoreEntriesScenarioWithConsensus() {
    fmt::print("oneLeaderOneFollowerMoreEntriesScenarioWithConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'1', 1},{'2', 2},{'3', 3},{'4', 2},{'5', 1},{'6', 3}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, false));
    launchServers(serverFiber, nodes);
    auto expectedLog = {MetaEntry{std::tuple{'1', 1}, 1}, MetaEntry{std::tuple{'2', 2}, 1},
                        MetaEntry{std::tuple{'3', 3}, 1}, MetaEntry{std::tuple{'4', 2}, 1},
                        MetaEntry{std::tuple{'5', 1}, 1}, MetaEntry{std::tuple{'6', 3}, 1}};
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 6 && node->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderManyFollowersScenarioWithConsensus() {
    fmt::print("oneLeaderManyFollowersScenarioWithConsensus\n");
    auto nodes = std::vector<std::unique_ptr<Node>>();
    for (auto _ : boost::irange(0, 12)) {
        nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    }
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, false));
    cooperative_scheduler::debug = false;
    launchServers(serverFiber, nodes);
    auto expectedLog = {MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1} };
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 2 && node->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus() {
    fmt::print("oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    for (auto _ : boost::irange(0, 4)) {
        nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    }
    nodes.push_back(std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, false));
    launchServers(serverFiber, nodes);
    auto expectedLog = {MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1} };
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 2 && node->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus() {
    fmt::print("oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus\n");
    auto entries1 = std::map<char, int> {{'a', 1},{'b', 2}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entries1, nodes, false));
    fmt::print("Term 1 - replicate entries1\n");
    launchServers(serverFiber, nodes);
    fmt::print("Term 2 - replicate entries2\n");
    auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
    auto leader = dynamic_cast<Leader*>(nodes.back()->me());
    leader->replicateEntries(entries2);
    launchServers(serverFiber, nodes);
    auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'b', 2}, 1},
                       MetaEntry{std::tuple{'c', 3}, 2}, MetaEntry{std::tuple{'d', 4}, 2}};
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 4 && node->currentTerm == 2);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerShouldCatchUpWithConsensus() {
    fmt::print("oneLeaderOneFollowerShouldCatchUpWithConsensus\n");
    auto entries1 = std::map<char, int> {{'a', 1},{'b', 2}};
    auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
    auto entries3 = std::map<char, int> {{'e', 5}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entries1, nodes, false));
    auto leader = dynamic_cast<Leader*>(nodes.back()->me());
    auto afollower = dynamic_cast<ArtificialFollower*>(nodes.front()->me());
    fmt::print("Term 1 - replicate entries1\n");
    launchServers(serverFiber, nodes);

    leader->replicateEntries(entries2);
    fmt::print("Term 2 - replicate entries2\n");
    launchServers(serverFiber, nodes);

    afollower->poison(1, {MetaEntry{std::tuple{'a', 1}, 1} });
    leader->replicateEntries(entries3);
    fmt::print("Term 3 - replicate entries3; follower log is going to be aligned\n");
    launchServers(serverFiber, nodes);

    auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'b', 2}, 1},
                       MetaEntry{std::tuple{'c', 3}, 2}, MetaEntry{std::tuple{'d', 4}, 2},
                       MetaEntry{std::tuple{'e', 5}, 3}};

    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 5 && node->currentTerm == 3);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus() {
    fmt::print("oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus\n");
    auto entries1 = std::map<char, int> {{'a', 1}};
    auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
    auto entries3 = std::map<char, int> {{'e', 5}};
    auto stopOnStateChange = true;
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, stopOnStateChange));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entries1, nodes, stopOnStateChange));
    auto leader = dynamic_cast<Leader*>(nodes.back()->me());
    auto afollower = dynamic_cast<ArtificialFollower*>(nodes.front()->me());
    fmt::print("Term 1 - replicate entries1\n");
    launchServers(serverFiber, nodes);

    leader->replicateEntries({});
    fmt::print("Term 2 - just bump Leader term\n");
    launchServers(serverFiber, nodes);

    leader->replicateEntries(entries2);
    fmt::print("Term 3 - replicate entries2\n");
    launchServers(serverFiber, nodes);

    afollower->poison(2, {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'z', 3}, 2} });
    leader->replicateEntries(entries3);
    fmt::print("Term 4 - replicate entries3; follower log is going to be aligned\n");
    launchServers(serverFiber, nodes);

    auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'c', 3}, 3},
                        MetaEntry{std::tuple{'d', 4}, 3}, MetaEntry{std::tuple{'e', 5}, 4}};
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 4 && node->currentTerm == 4);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus() {
    fmt::print("oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus\n");
    auto entries1 = std::map<char, int> {{'a', 1}};
    auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
    auto entries3 = std::map<char, int> {{'e', 5}};
    auto stopOnStateChange = true;
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, stopOnStateChange));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entries1, nodes, stopOnStateChange));
    auto leader = dynamic_cast<Leader*>(nodes.back()->me());
    auto afollower = dynamic_cast<ArtificialFollower*>(nodes.front()->me());
    fmt::print("Term 1 - replicate entries1\n");
    launchServers(serverFiber, nodes);

    leader->replicateEntries({});
    fmt::print("Term 2 & 3 - just bump Leader term\n");
    launchServers(serverFiber, nodes);
    leader->replicateEntries({});
    launchServers(serverFiber, nodes);

    leader->replicateEntries(entries2);
    fmt::print("Term 4 - replicate entries2\n");
    launchServers(serverFiber, nodes);
    afollower->poison(4, {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'b', 1}, 1},
                       MetaEntry{std::tuple{'x', 2}, 2}, MetaEntry{std::tuple{'z', 2}, 2},
                       MetaEntry{std::tuple{'p', 3}, 3}, MetaEntry{std::tuple{'q', 3}, 3},
                       MetaEntry{std::tuple{'c', 3}, 4}, MetaEntry{std::tuple{'d', 4}, 4}});
    leader->replicateEntries(entries3);
    fmt::print("Term 5 - replicate entries3; follower log is going to be aligned\n");
    launchServers(serverFiber, nodes);

    auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'c', 3}, 4},
                         MetaEntry{std::tuple{'d', 4}, 4}, MetaEntry{std::tuple{'e', 5}, 5}};
    boost::for_each(nodes, [&expectedLog](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(expectedLog));
        assert(node->commitIndex == 4 && node->currentTerm == 5);
    });
    fmt::print("\n");
}

static void oneFailingLeaderOneFollowerScenarioWithNoConsensus() {
    fmt::print("oneFailingLeaderOneFollowerScenarioWithNoConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    auto stopOnStateChange = true;
    auto delayFollower = true;
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, entriesToReplicate, nodes, stopOnStateChange,
                                             delayFollower));
     // at this point we need to have followers
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, stopOnStateChange));

    fmt::print("Term 1 - HeartBeat is delayed, all servers become candidates\n");
    launchServers(serverFiber, nodes);
    auto oldLeader = dynamic_cast<Candidate*>(nodes.back()->me());
    auto oldFollower = dynamic_cast<Candidate*>(nodes.front()->me());
    assert(oldLeader->state == State::CANDIDATE && oldFollower->state == State::CANDIDATE);
    fmt::print("\n");
}

static void oneFailingLeaderOneFollowerScenarioWithConsensus() {
    fmt::print("oneFailingLeaderOneFollowerScenarioWithConsensus\n");
    cooperative_scheduler::debug = false;
    ::debug = false;
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    auto stopOnStateChange = true;
    auto delayFollower = true;
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, entriesToReplicate, nodes, stopOnStateChange,
                                             delayFollower));
    nodes.push_back(std::make_unique<Server>(Instance::LEADER, entriesToReplicate, nodes, stopOnStateChange));

    fmt::print("Term 1 - HeartBeat is delayed, all servers become candidates\n");
    launchServers(serverFiber, nodes);
    auto oldLeader = dynamic_cast<Candidate*>(nodes.back()->me());
    auto oldFollower = dynamic_cast<Candidate*>(nodes.front()->me());
    assert(oldLeader->state == State::CANDIDATE && oldFollower->state == State::CANDIDATE);
    fmt::print("Term 2 - one wins elections and replicate entries\n");
    launchServers(serverFiber, nodes);
    boost::for_each(nodes, [](auto &&server){
        auto node = server->me();
        assert(node->verifyLog({MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1}}));
        assert(node->commitIndex == 2 && node->currentTerm == 1);
        assert(node->state == State::DONE);
    });
    fmt::print("\n");
}

static void twoCandidatesInitiateElectionsOneWins() {
    fmt::print("twoCandidatesInitiateElectionsOneWins\n");
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    fmt::print("All servers become candidates, one wins elections\n");
    launchServers(serverFiber, nodes);
    auto follower1 = dynamic_cast<Follower*>(nodes.front()->me());
    auto follower2 = dynamic_cast<Follower*>(nodes.back()->me());
    assert(follower1->state == State::DONE && follower2->state == State::DONE);
    fmt::print("\n");
}

static void moreCandidatesInitiateElectionsOneWins() {
    fmt::print("moreCandidatesInitiateElectionsOneWins\n");
    auto nodes = std::vector<std::unique_ptr<Node>>();
    for (auto _ : boost::irange(0, 5)) {
        nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, std::nullopt, nodes, false));
    }
    fmt::print("All servers become candidates, one wins elections\n");
    launchServers(serverFiber, nodes);
    boost::for_each(nodes, [](auto &&server){
        auto node = server->me();
        assert(node->state == State::DONE);
    });
    fmt::print("\n");
}

static void twoCandidatesInitiateElectionsOneWinsWithConsensus() {
    fmt::print("twoCandidatesInitiateElectionsOneWinsWithConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'1', 1},{'2', 2},{'3', 3},{'4', 4},{'5', 5},{'6', 6}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, entriesToReplicate, nodes, false));
    nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, entriesToReplicate, nodes, false));
    fmt::print("All servers become candidates, one wins elections and replicate entries\n");
    launchServers(serverFiber, nodes);
    boost::for_each(nodes, [](auto &&server){
        auto node = server->me();
        assert(node->verifyLog({MetaEntry{std::tuple{'1', 1}, 1}, MetaEntry{std::tuple{'2', 2}, 1},
                                MetaEntry{std::tuple{'3', 3}, 1}, MetaEntry{std::tuple{'4', 4}, 1},
                                MetaEntry{std::tuple{'5', 5}, 1}, MetaEntry{std::tuple{'6', 6}, 1}}));
        assert(node->commitIndex == 6 && node->currentTerm == 1);
        assert(node->state == State::DONE);
    });
    fmt::print("\n");
}

static void moreCandidatesInitiateElectionsOneWinsWithConsensus() {
    fmt::print("moreCandidatesInitiateElectionsOneWinsWithConsensus\n");
    auto entriesToReplicate = std::map<char, int> {{'1', 1},{'2', 2},{'3', 3},{'4', 4},{'5', 5},{'6', 6}};
    auto nodes = std::vector<std::unique_ptr<Node>>();
    for (auto _ : boost::irange(0, 5)) {
        nodes.push_back(std::make_unique<Server>(Instance::FOLLOWER, entriesToReplicate, nodes, false));
    }
    fmt::print("All servers become candidates, one wins elections and replicate entries\n");
    launchServers(serverFiber, nodes);
    boost::for_each(nodes, [](auto &&server){
        auto node = server->me();
        assert(node->verifyLog({MetaEntry{std::tuple{'1', 1}, 1}, MetaEntry{std::tuple{'2', 2}, 1},
                                MetaEntry{std::tuple{'3', 3}, 1}, MetaEntry{std::tuple{'4', 4}, 1},
                                MetaEntry{std::tuple{'5', 5}, 1}, MetaEntry{std::tuple{'6', 6}, 1}}));
        assert(node->commitIndex == 6 && node->currentTerm == 1);
        assert(node->state == State::DONE);
    });
    fmt::print("\n");
}

static int getRandom(unsigned from, unsigned to) {
    static std::random_device device;
    static std::mt19937 generator(device());
    std::uniform_int_distribution<> random(from, to);
    return random(generator);
}

static std::vector<MetaEntry> generateRandomLog(unsigned size, int maxTerm)  {
    //using namespace boost::adaptors;
    auto randomLog = std::vector<MetaEntry>();
    for (auto _ : boost::irange(0u, size)) {
        auto i = getRandom(1, maxTerm);
        randomLog.push_back(MetaEntry{std::tuple{'a', i}, i});
    }
#if 0 // FIXME
    boost::sort(randomLog);
#endif
    return randomLog;
}

static void stressTest() {
    using namespace boost::adaptors;
    const auto logSize = 10u;
    auto logToPoison = generateRandomLog(logSize, 10u);
    fmt::print("stressTest:    size= {}\n", logSize);
    printLog(logToPoison);
    auto nodes = std::vector<std::unique_ptr<Node>>();
    for (auto _ : boost::irange(0u, 15u)) {
        auto delayRandomly = static_cast<bool>(getRandom(0u, 1u));
        auto follower = std::make_unique<Server>(Instance::ARTIFICIAL_FOLLOWER, std::nullopt, nodes, false, delayRandomly);
        auto afollower = dynamic_cast<ArtificialFollower*>(follower->me());
        auto filteredLog = boost::copy_range<std::vector<MetaEntry>>(logToPoison | filtered([](auto &&){
                                return static_cast<bool>(getRandom(0u, 1u)); }));
        afollower->poison(1, filteredLog);
        nodes.push_back(std::move(follower));
    }
    fmt::print("All servers become candidates, eventually one of injected log should be replicated to all\n");
    launchServers(serverFiber, nodes);

    auto it = boost::range::find_if(nodes, [](auto &&node) { return dynamic_cast<Leader*>(node->me()) != nullptr; });
    assert(it != nodes.end());
    auto leader = dynamic_cast<Leader*>((*it)->me());
    auto leaderLog = leader->getCurrentLog();
    boost::for_each(nodes, [&leaderLog, logSize](auto &&server){
        auto node = server->me();
        assert(node->verifyLog(leaderLog));
        assert(node->commitIndex == logSize && node->state == State::DONE);
    });
    fmt::print("\n");
}

int main() {
   oneLeaderOneFollowerScenarioWithConsensus();
   oneLeaderOneFollowerMoreEntriesScenarioWithConsensus();
   oneLeaderManyFollowersScenarioWithConsensus();
   oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus();
   oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus();
   oneLeaderOneFollowerShouldCatchUpWithConsensus();
   oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus();
   oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus();
   oneFailingLeaderOneFollowerScenarioWithNoConsensus();
   oneFailingLeaderOneFollowerScenarioWithConsensus();
//   twoCandidatesInitiateElectionsOneWins();
//   moreCandidatesInitiateElectionsOneWins();
//   twoCandidatesInitiateElectionsOneWinsWithConsensus();
//   moreCandidatesInitiateElectionsOneWinsWithConsensus();
//   stressTest();
   return 0;
}
