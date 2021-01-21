#include "cooperative_scheduler.hh"
#include "channel.hh"
#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
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

struct MetaEntry {
    MetaEntry(const std::tuple<char, int> &_entry, int _term) noexcept
        : entry(_entry), term(_term) {}
    std::tuple<char, int> entry;
    int term;
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

class Node {
public:
    virtual void run() = 0;
    virtual ~Node() = default;

    void trackLog() const {
        if (debug) {
            for (auto &&item : log) {
                fmt::print("({}, {}) {}  ", item.term, std::get<0>(item.entry), std::get<1>(item.entry));
            }
            fmt::print("\n");
        }
    }

protected:
    std::map<char, int> logState;
    std::vector<MetaEntry> log;
    State state = State::INITIAL;
public:
    int currentTerm = 0, commitIndex = 0, lastApplied = 0;
    constexpr static bool debug = true;
};

class Follower : public Node {
public:
    void run() override {
        state = State::INITIAL;
        currentTerm++;
        const auto logSize = static_cast<int>(log.size());
        commitIndex = std::max(logSize, 0);
        const void *me = this;
        fmt::print("Follower {}: starts\n", me);
        while (!done(*receiveHeartbeat().get())) {
            auto appendEntries = receiveAppendEntriesReq().get();
            auto apply = true;
            auto prevIndex = appendEntries->prevIndex;
            auto prevTerm = appendEntries->prevTerm;
            // [1]
            if (appendEntries->term < currentTerm) {
                apply = false;
            } else {
                // [1]
                if (appendEntries->term > currentTerm) {
                   currentTerm = appendEntries->term;
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
                        if (log[prevIndex + 1].term != appendEntries->metaEntry.term) {
                            // cleanup because of inconsistency, leave only log[0..prevIndex] prefix,
                            // with assumption that prefix is valid we can append entry in this iteration
                            shrinkUntil(prevIndex);
                        }
                    }
                }
            }
            auto appendEntries_cp = std::make_unique<AppendEntriesReq>(*appendEntries);
            // at this point previous msg from channel ends lifetime so we need copy it
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, apply)).get();
            auto [id, value] = appendEntries_cp->metaEntry.entry;
            auto leaderCommit = appendEntries_cp->leaderCommit;
            if (apply) {
                // [4]
                log.emplace_back(appendEntries_cp->metaEntry.entry, appendEntries_cp->metaEntry.term);
                logState[id] = value;
                lastApplied++;
                fmt::print("Follower {}: {} := {}\n", me, id, value);
                // [5]
                commitIndex = std::min(leaderCommit + 1, commitIndex + 1);
            } else {
                fmt::print("Follower {}: no consensus for {}\n", me, id);
                trackLog();
            }
        }
        state = State::DONE;
        fmt::print("Follower {}: done with commitIndex = {}\n", me, commitIndex);
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
        }
        log.erase(log.begin() + index + 1, log.end());
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
            boost::for_each(followers, [this, metaEntry = MetaEntry{entry, currentTerm}, followerIsDone = false](auto &&follower) mutable {
                 assert(follower != nullptr);
                 auto &it = *follower;
                 do {
                    auto [id, value] = metaEntry.entry;
                    const auto logSize = static_cast<int>(log.size());
                    auto prevIndex = std::min(replicaNextIndex(&it) - 1, logSize - 1);
                    auto prevTerm = (prevIndex >= 0)? log[prevIndex].term  : 0;
                    it.sendAppendEntriesReq(AppendEntriesReq(metaEntry, currentTerm, prevIndex, prevTerm, commitIndex)).get();
                    auto response = *it.receiveAppendEntriesResp().get();
                    auto expected = AppendEntriesResp(currentTerm, true);

                    if (response <=> expected != 0) {
                        fmt::print("Leader: No consensus for {} {}\n", id, value);
                        // [5.3]
                        nextIndex[&it] = replicaNextIndex(&it) - 1;
                        if (replicaNextIndex(&it) >= 0) {
                            metaEntry = log[replicaNextIndex(&it)];
                        }
                        it.sendHeartbeat(false).get();
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
                        it.sendHeartbeat(false).get();
                    }
                 } while (!followerIsDone);
            });
            logState[id] = value;
            // [5.3]
            boost::for_each(followers, [this](auto &&follower){
                auto it = follower.get();
                matchIndex[it] = replicaNextIndex(it);
                nextIndex[it] = replicaNextIndex(it) + 1;
            });
            // [5.3] [5.4]
            using namespace boost::adaptors;
            auto indexList =  boost::copy_range<std::vector<int>>(matchIndex | map_values);
            boost::range::sort(indexList);
            auto majorityCommitIndex = (log[indexList[indexList.size()/2]].term == currentTerm)? indexList[indexList.size()/2] :0;
            commitIndex = std::max(majorityCommitIndex, commitIndex + 1);
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
         return (it != nextIndex.end())? it->second : (throw std::logic_error("Shouldn't happen"), 42);
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
