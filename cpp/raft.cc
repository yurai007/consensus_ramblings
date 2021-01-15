#include "cooperative_scheduler.hh"
#include "channel.hh"
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/irange.hpp>
#include <fmt/core.h>
#include <tuple>
#include <map>
#include <vector>
#include <cassert>
#include <future>
#include <stdexcept>
#include <compare>
#include <memory>

struct Message {
    virtual ~Message() = default;
};

#ifndef __clang__
    static_assert(CoroutineChannel<channel, Message*>, "it's important for implementation to have fully preemptable coroutine channel");
#endif

enum class State { INITIAL, DONE };

struct HeartBeat final : Message {
    HeartBeat(bool _done) noexcept : done(_done) {}
    bool done = false;
};

struct AppendEntriesReq final : Message {
    AppendEntriesReq(const std::tuple<char, int> &_entry, int _term, int _prevIndex, int _prevTerm, int _leaderCommit) noexcept
        : entry(_entry), term(_term), prevIndex(_prevIndex), prevTerm(_prevTerm), leaderCommit(_leaderCommit) {}
    std::tuple<char, int> entry;
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

struct MetaEntry {
    MetaEntry(const std::tuple<char, int> &_entry, int _term) noexcept
        : entry(_entry), term(_term) {}
    std::tuple<char, int> entry;
    int term;
};

class Node {
public:
    virtual void run() = 0;
    virtual ~Node() = default;

    void trackLog() const {
        if (debug) {
            throw std::logic_error("Unimplemented yet");
        }
    }

protected:
    std::map<char, int> logState;
    std::vector<MetaEntry> log;
    State state = State::INITIAL;
public:
    int currentTerm = 0, commitIndex = 0, lastApplied = 0;
    constexpr static bool debug = false;
};

class Follower : public Node {
public:
    void run() override {
        state = State::INITIAL;
        currentTerm++;
        const auto logSize = static_cast<int>(log.size());
        commitIndex = std::max(logSize - 1, 0);
        const void *me = this;
        fmt::print("Follower {}: starts\n", me);
        while (!done(*receiveHeartbeat().get())) {
            auto appendEntries = receiveAppendEntriesReq().get();
            auto apply = true;
            auto prevIndex = appendEntries->prevIndex;
            auto prevTerm = appendEntries->prevTerm;
            if (prevIndex >= 0) {
                if (prevIndex < logSize) {
                    // [2]
                    if (log[prevIndex].term != prevTerm) {
                        apply = false;
                    }
                } else {
                    apply = false;
                }
                if (prevIndex + 1 < logSize) {
                    // [3]
                    if (log[prevIndex + 1].term != appendEntries->term) {
                        // cleanup because of inconsistency, leave only log[0..prevIndex] prefix,
                        // with assumption that prefix is valid we can append entry in this iteration
                        shrinkUntil(prevIndex);
                    }
                }
            }
            auto appendEntries_cp = std::make_unique<AppendEntriesReq>(*appendEntries);
            // at this point previous msg from channel ends lifetime so we need copy it
            sendAppendEntriesResp(AppendEntriesResp(appendEntries->term, apply)).get();
            auto [id, value] = appendEntries_cp->entry;
            if (apply) {
                log.emplace_back(appendEntries_cp->entry, appendEntries_cp->term);
                logState[id] = value;
                fmt::print("Follower {}: {} := {}\n", me, id, value);
                commitIndex++;
                lastApplied++;
                if (appendEntries_cp->term > currentTerm) {
                    currentTerm = appendEntries_cp->term;
                }
            } else {
                fmt::print("Follower {}: no consensus for {}\n", me, id);
                trackLog();
            }
        }
        state = State::DONE;
        fmt::print("Follower {}: done\n", me);
        trackLog();
    }
    bool verifyLog(const std::vector<MetaEntry>&) const { return true; }
    virtual ~Follower() = default;
private:
    channel<Message*> channelToLeader;
    std::unique_ptr<Message> msg;
    Message *msg_p = nullptr;

   void shrinkUntil(int index) {
        for (auto i : boost::irange(index + 1, static_cast<int>(log.size()))) {
            auto &&[keyValue, _] = log[i];
            auto &&[key, __] = keyValue;
            logState.erase(key);
            // log.removeAt(i); FIXME
        }
    }

    bool done(const HeartBeat &_msg) const {
        return _msg.done;
    }
public:
    std::future<int> sendHeartbeat(bool done) {
        msg = std::make_unique<HeartBeat>(done);
        msg_p = msg.get();
        co_await channelToLeader.write(msg_p);
        co_return 0;
    }

    std::future<HeartBeat*> receiveHeartbeat() {
        auto [_msg, ok] = co_await channelToLeader.read();
        co_return dynamic_cast<HeartBeat*>(_msg);
    }

    std::future<int> sendAppendEntriesReq(AppendEntriesReq &&req) {
        msg = std::make_unique<AppendEntriesReq>(req);
        msg_p = msg.get();
        co_await channelToLeader.write(msg_p);
        co_return 0;
    }

    std::future<AppendEntriesReq*> receiveAppendEntriesReq() {
        auto [_msg, ok] = co_await channelToLeader.read();
        co_return dynamic_cast<AppendEntriesReq*>(_msg);
    }

     std::future<int> sendAppendEntriesResp(AppendEntriesResp &&rep) {
         msg = std::make_unique<AppendEntriesResp>(rep);
         msg_p = msg.get();
         co_await channelToLeader.write(msg_p);
         co_return 0;
     }

    std::future<AppendEntriesResp*> receiveAppendEntriesResp() {
        auto [_msg, ok] = co_await channelToLeader.read();
        co_return dynamic_cast<AppendEntriesResp*>(_msg);
    }
};

class Leader final : public Node {
public:
    Leader(std::vector<std::shared_ptr<Follower>> &_followers, const std::map<char, int> &_entriesToReplicate) :
        followers(_followers),
        entriesToReplicate(_entriesToReplicate) {
        boost::for_each(followers, [this](auto &&follower){
            nextIndex[follower.get()] = 0;
        });
    }

    void run() override {
        state = State::INITIAL;
        currentTerm++;
        fmt::print("Leader of term {}\n", currentTerm);
        boost::for_each(entriesToReplicate, [this](auto &entry){
            auto [id, value] = entry;
            boost::for_each(followers, [](auto &&follower){
                follower->sendHeartbeat(false).get(); // FIXME: do in parallel + when_all helper
            });
            lastApplied++;
            log.emplace_back(entry, currentTerm);
            // x = x, as workaround for http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/p0588r1.html
            boost::for_each(followers, [this, entry = std::make_tuple(entry.first, entry.second), id = id, value = value, followerIsDone = false](auto &&follower) mutable {
                 assert(follower != nullptr);
                 auto &it = *follower;
                 do {
                    const auto logSize = static_cast<int>(log.size());
                    auto prevIndex = std::min(replicaNextIndex(&it) - 1, logSize - 1);
                    auto prevTerm = (prevIndex >= 0)? log[prevIndex].term  : 0;
                    auto term =  (prevIndex + 1 >= 0 && prevIndex + 1 < logSize) ? log[prevIndex + 1].term  : currentTerm;
                    it.sendAppendEntriesReq(AppendEntriesReq(entry, term, prevIndex, prevTerm, commitIndex)).get();
                    auto response = *it.receiveAppendEntriesResp().get();
                    auto expected = AppendEntriesResp(term, true);

                     if (response <=> expected != 0) {
                        fmt::print("Leader: No consensus for {} {}\n", id, value);
                        nextIndex[&it] = replicaNextIndex(&it) - 1;
                        if (replicaNextIndex(&it) >= 0) {
                            entry = log[replicaNextIndex(&it)].entry;
                        }
                        it.sendHeartbeat(false).get();
                        trackLog();
                    } else if (term == currentTerm) {
                        followerIsDone = true;
                    } else {
                        nextIndex[&it] = replicaNextIndex(&it) + 1;
                        if (replicaNextIndex(&it) < logSize) {
                            entry = log[replicaNextIndex(&it)].entry;
                        } else {
                            entry = std::tie(id, value);
                        }
                        it.sendHeartbeat(false).get();
                    }

                 } while (!followerIsDone);
            });
            commitIndex++;
            logState[id] = value;
            boost::for_each(followers, [this](auto &&follower){
                nextIndex[follower.get()] = replicaNextIndex(follower.get()) + 1;
            });
            fmt::print("Leader: {} := {}\n", id, value);
         });
        boost::for_each(followers, [this](auto &&follower){
            follower->sendHeartbeat(true).get();
        });
        fmt::print("Leader: done\n");
    }

    void replicateEntries(const std::map<char, int> &entriesToReplicate_) {
        entriesToReplicate = entriesToReplicate_;
    }

private:
     std::vector<std::shared_ptr<Follower>> &followers;
     std::map<Follower*, int> nextIndex, matchIndex;
     std::map<char, int> entriesToReplicate;

     int replicaNextIndex(Follower *follower) const {
         auto it = nextIndex.find(follower);
         return (it != nextIndex.end())? *it : throw std::logic_error("Shouldn't happen"), 0;
     }
};

class ArtificialFollower : public Follower {
public:
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

static void leaderFiber(Leader *leader) {
    auto task = [leader]() -> std::future<int> {
        try {
            leader->run();
        } catch (const std::exception &e) { fmt::print("Leader: failed with {}\n", e.what());}
        co_return 0;
    };
    task().get();
}

static void followerFiber(Follower *follower) {
    auto task = [follower]() -> std::future<int> {
         try {
             follower->run();
         } catch (...) {  fmt::print("Follower: failed\n"); }
         co_return 0;
    };
    task().get();
}

template<class... Args>
static void launchLeaderAndFollowers(Args&&... args) {
    cooperative_scheduler{ std::forward<Args>(args)...};
}

static void oneLeaderOneFollowerScenarioWithConsensus() {
    fmt::print("oneLeaderOneFollowerScenarioWithConsensus\n");
    std::vector followers = {std::make_shared<Follower>()};
    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto leader = Leader(followers, entriesToReplicate);
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);
    auto expectedLog = {MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1} };
    boost::for_each(followers, [&expectedLog](auto &&follower){
        assert(follower->verifyLog(expectedLog));
        assert(follower->commitIndex == 2 && follower->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderManyFollowersScenarioWithConsensus() {
    fmt::print("oneLeaderManyFollowersScenarioWithConsensus\n");
    auto followers = std::vector<std::shared_ptr<Follower>>(12u);
    boost::for_each(followers, [](auto &follower){
        follower = std::make_shared<Follower>();
    });

    auto entriesToReplicate = std::map<char, int> {{'x', 1},{'y', 2}};
    auto leader = Leader(followers, entriesToReplicate);
    cooperative_scheduler::debug = false;
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0], followerFiber, *followers[1],
            followerFiber, *followers[2], followerFiber, *followers[3], followerFiber, *followers[4], followerFiber, *followers[5],
            followerFiber, *followers[6], followerFiber, *followers[7],
            followerFiber, *followers[8], followerFiber, *followers[9], followerFiber, *followers[10], followerFiber, *followers[11]);
    auto expectedLog = {MetaEntry{std::tuple{'x', 1}, 1}, MetaEntry{std::tuple{'y', 2}, 1} };
    boost::for_each(followers, [&expectedLog](auto &&follower){
        assert(follower->verifyLog(expectedLog));
        assert(follower->commitIndex == 2 && follower->currentTerm == 1);
    });
    fmt::print("\n");
}

static void oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus() {
    fmt::print("oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus\n");
    auto entries1 = std::map<char, int> {{'a', 1}};
    auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
    auto entries3 = std::map<char, int> {{'e', 5}};
    //auto stopOnStateChange = true
    auto afollower = std::make_shared<ArtificialFollower>();
    auto followers = std::vector<std::shared_ptr<Follower>>{afollower};
    auto leader = Leader(followers, entries1);
    fmt::print("Term 1 - replicate entries1\n");
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

    leader.replicateEntries({});
    fmt::print("Term 2 - just bump Leader term\n");
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

    leader.replicateEntries(entries2);
    fmt::print("Term 3 - replicate entries2\n");
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

    afollower->poison(2, {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'z', 3}, 2} });
    leader.replicateEntries(entries3);
    fmt::print("Term 4 - replicate entries3; follower log is going to be aligned\n");
    launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

    auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'d', 4}, 3},
         MetaEntry{std::tuple{'c', 3}, 3}, MetaEntry{std::tuple{'e', 5}, 4}};
    boost::for_each(followers, [&expectedLog](auto &&follower){
        assert(follower->verifyLog(expectedLog));
        assert(follower->commitIndex == 4 && follower->currentTerm == 4);
    });
}

static void oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus() {
     fmt::print("oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus\n");
     auto entries1 = std::map<char, int> {{'a', 1}};
     auto entries2 = std::map<char, int> {{'c', 3},{'d', 4}};
     auto entries3 = std::map<char, int> {{'e', 5}};
     auto afollower = std::make_shared<ArtificialFollower>();
     auto followers = std::vector<std::shared_ptr<Follower>>{afollower};
     auto leader = Leader(followers, entries1);
     fmt::print("Term 1 - replicate entries1\n");
     launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

     leader.replicateEntries({});
     fmt::print("Term 2 & 3 - just bump Leader term\n");
     launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);
     leader.replicateEntries({});
     launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

     leader.replicateEntries(entries2);
     fmt::print("Term 4 - replicate entries2");
     launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

     afollower->poison(4, {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'b', 1}, 1},
                        MetaEntry{std::tuple{'x', 2}, 2}, MetaEntry{std::tuple{'z', 2}, 2}, MetaEntry{std::tuple{'p', 3}, 3},
                        MetaEntry{std::tuple{'q', 3}, 3}, MetaEntry{std::tuple{'c', 3}, 4}, MetaEntry{std::tuple{'d', 4}, 4}});
     leader.replicateEntries(entries3);
     fmt::print("Term 5 - replicate entries3; follower log is going to be aligned");
     launchLeaderAndFollowers(leaderFiber, leader, followerFiber, *followers[0]);

     auto expectedLog = {MetaEntry{std::tuple{'a', 1}, 1}, MetaEntry{std::tuple{'d', 4}, 4},
          MetaEntry{std::tuple{'c', 3}, 4}, MetaEntry{std::tuple{'e', 5}, 5}};
     boost::for_each(followers, [&expectedLog](auto &&follower){
         assert(follower->verifyLog(expectedLog));
         assert(follower->commitIndex == 4 && follower->currentTerm == 5);
     });
 }

int main() {
    oneLeaderOneFollowerScenarioWithConsensus();
    oneLeaderManyFollowersScenarioWithConsensus();
    oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus();
    oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus();
    return 0;
}
