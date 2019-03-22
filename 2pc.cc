#include <experimental/thread_pool>
#include <iostream>
#include <cassert>
#include <type_traits>

// TODO: without mutable and move it's ill-formed with wall of text :(

namespace execution = std::experimental::execution;
using std::experimental::static_thread_pool;
template<class T>
using future = execution::executor_future_t<static_thread_pool::executor_type, T>;
template<class T>
using promise = std::experimental::executors_v1::promise<T>;

enum class State {
    INITIAL, PROPOSE, VOTE, COMMIT_OR_ABORT
};

class Node {
public:
    virtual void run() = 0;
    virtual ~Node() = default;
protected:
    int register_ = 0;
    State state = State::INITIAL;
};

class Replica final : public Node {
public:
    Replica(bool should_commit_) noexcept : should_commit(should_commit_) {}

    void run() override {
        assert(state == State::INITIAL);
        auto result = read().then([this](auto proposed_value){
            state = State::PROPOSE;
            return write(static_cast<int>(should_commit)).then([this, proposed_value = std::move(proposed_value)](auto _) mutable {
                state = State::VOTE;
                return read().then([this, proposed_value = std::move(proposed_value)](auto commit_reply) mutable {
                    state = State::COMMIT_OR_ABORT;
                    if (static_cast<bool>(commit_reply.get())) {
                        register_ = proposed_value.get();
                        std::cout << "Replica: Value := " << register_ << "\n";
                    } else {
                        std::cout << "No consensus\n";
                    }
                    return std::move(proposed_value);
                });
            });
        });
        result.get();
    }

    future<int> read() {
        promise<int> p;
        p.set_value(42);
        return p.get_future();
    }

    future<int> write(int) {
        promise<int> p;
        p.set_value(24);
        return p.get_future();
    }

private:
    const bool should_commit;
};

template<typename T>
struct extract
{
    using value_type = T;
};

template<template<typename, typename ...> class X, typename T, typename ...Args>
struct extract<X<T, Args...>>
{
    using value_type = T;
};

template<class FuturesContainer>
inline auto
when_all(FuturesContainer &&container) {
    using Fut = std::decay_t<typename FuturesContainer::value_type>;
    if (std::all_of(container.begin(), container.end(),
                    [](auto &&f) {
                        return f.is_ready(); })) {
        promise<FuturesContainer> p;
        p.set_value(std::move(container));
        return p.get_future();
    } else {
        throw;
    }
}

class Leader final : public Node {
public:
    Leader(const std::vector<Replica> &replicas_, int proposed_) : replicas(replicas_), proposed(proposed_) {}

    void run() override {
        assert(state == State::INITIAL);
        state = State::PROPOSE;
        std::vector<future<int>> channels;
        for (auto &&replica : replicas) {
            channels.emplace_back(replica.write(proposed));
        }
        auto result = when_all(std::move(channels)).then([this](auto channels_) mutable {
            auto channels = channels_.get();
            channels.clear();
            state = State::VOTE;
            for (auto &&replica : replicas) {
                channels.emplace_back(replica.read());
            }
            return when_all(std::move(channels)).then([this](auto channels_) mutable {
                auto channels = channels_.get();
                channels.clear();
                state = State::COMMIT_OR_ABORT;
                auto maybe_commit = std::all_of(channels.begin(), channels.end(), [](auto &&f){
                    return static_cast<bool>(f.get());
                });
                for (auto &&replica : replicas) {
                    channels.emplace_back(replica.write(maybe_commit));
                }
                return when_all(std::move(channels)).then([this, maybe_commit](auto channels) mutable noexcept {
                    if (maybe_commit) {
                        register_ = proposed;
                        std::cout << "Replica: Value := " << register_ << "\n";
                    } else {
                        std::cout << "No consensus\n";
                    }
                    state = State::INITIAL;
                    return std::move(channels);
                });
            });
        });
        result.get();
    }
private:
    std::vector<Replica> replicas;
    const int proposed;
};

static auto minimal_then_test() {
    static_thread_pool pool(1);
    auto f = execution::require(pool.executor(), execution::twoway).twoway_execute([](){
        return 42;
    }).then([](auto maybe_value){
        auto value = maybe_value.get();
        return ++value;
    }).then([](auto maybe_value){
        auto value = maybe_value.get();
        return ++value;
    });
    std::cout << f.get() << "\n";
}

static auto one_leader_one_replica_scenario_with_consensus() {
    auto replica = Replica(true);
    auto leader = Leader({replica}, 123);
    //replica.run();
    leader.run();
}

int main() {
    minimal_then_test();
    one_leader_one_replica_scenario_with_consensus();
    return 0;
}
