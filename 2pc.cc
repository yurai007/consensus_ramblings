﻿#include <experimental/thread_pool>
#include <boost/range/algorithm/for_each.hpp>
#include <iostream>
#include <cassert>
#include <type_traits>
#include <stdexcept>
#include <mutex>
#include <queue>
#include <condition_variable>

/*
 * Even with minimal_then_test helgrind complains a lot - implementation bug?
 * Without mutable and move it's ill-formed with wall of text :(
 * make_ready_future() is broken - implementation bug?
   Dummy make_ready_future through thread pool was used.
 * std_thread_safe_queue is not CopyConstructible nor MoveConstructible because of std:conditional_variable
   explicit '= delete' improves diagnostics a lot (gcc)
 * to warkaround above std::array was used
*/

namespace execution = std::experimental::execution;
using std::experimental::static_thread_pool;
template<class T>
using future = execution::executor_future_t<static_thread_pool::executor_type, T>;
template<class T>
using promise = std::experimental::executors_v1::promise<T>;

template <typename T>
struct is_future : std::true_type {};

//template <typename T>
//struct is_future<future<T>> : std::true_type {};

#if 0
/*
 * First one is broken - from some reason setting before getting cause Asan complains
 * Second one is not correct - returned future is not ready
*/
template<class T>
[[nodiscard]]
inline auto make_ready_future__broken(T &&value) {
    promise<T> p;
    p.set_value(value);
    return p.get_future();
}

template<class T>
[[nodiscard]]
inline auto make_ready_future__invalid(T &&value) {
    return execution::require(pool.executor(), execution::never_blocking).twoway_execute([value]{
        return value;
    });
}
#endif

static_thread_pool pool(2);

template<class T>
[[nodiscard]]
inline auto make_ready_future(T &&value) {
    promise<T> p;
    auto f = p.get_future();
    p.set_value(value);
    return f;
}

#ifndef __clang__
template <typename T>
concept bool Future = is_future<T>::value;

template<class FuturesContainer>
requires requires (FuturesContainer c) {
    std::begin(c);
    std::end(c);
    Future<decltype(*std::begin(c))>;
}
inline auto
when_all(FuturesContainer &&container) {
    using Fut = std::decay_t<typename FuturesContainer::value_type>;
    // fast path
    if (std::all_of(container.begin(), container.end(),
                    [](auto &&f) {
                        return f.is_ready(); })) {
        promise<FuturesContainer> p;
        auto f = p.get_future();
        p.set_value(std::move(container));
        return f;
    } else {
        // slow path
        future<FuturesContainer> f = execution::require(pool.executor(), execution::twoway).twoway_execute([container = std::move(container)]() mutable {
            FuturesContainer c;
            for (auto &&f : container) {
                auto item = f.get();
                c.emplace_back(std::move(make_ready_future(std::move(item))));
           }
            return std::move(c);
        });
        return f;
    }
}
#endif

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
    Replica(const Replica& replica) = delete;

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
                    return make_ready_future(0);
                });
            });
        });
        result.get();
    }

    future<int> read() {
        auto value = *channel.front_and_pop();
        std::cout << "read " << value << "\n";
        return make_ready_future(std::move(value));
    }

    future<int> write(int value) {
        std::cout << "write " << value << "\n";
        channel.push(value);
        return make_ready_future(std::move(value));
    }

private:
    const bool should_commit;

    template<class T>
    class std_thread_safe_queue
    {
    public:
        std_thread_safe_queue() = default;
        std_thread_safe_queue(const std_thread_safe_queue&) = delete;
        std_thread_safe_queue& operator=(std_thread_safe_queue&) = delete;

        void push(T new_value)
        {
            std::lock_guard lock(mutex_);
            queue_.push(std::move(new_value));
            cv_.notify_one();
        }

        std::shared_ptr<T> front_and_pop()
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this]{ return !queue_.empty(); });
            auto result = std::make_shared<T>(std::move(queue_.front()));
            queue_.pop();
            return result;
        }

        bool empty() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return queue_.empty();
        }
    private:
        mutable std::mutex mutex_;
        std::queue<T> queue_;
        std::condition_variable cv_;
    };
    std_thread_safe_queue<int> channel;
};

class Leader final : public Node {
public:
    Leader(std::array<Replica, 1> &replicas_, int proposed_) : replicas(replicas_), proposed(proposed_) {}

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
    std::array<Replica, 1> &replicas;
    const int proposed;
};

static auto returns_test() {
    future<int> f;
    return f;
}

static auto basic_tests() {
    {
        auto f = returns_test();
        std::vector<future<int>> channels(6); // = {std::move(f)};

#if 0   // why the hell CC is used here and everything is ill-formed ?
        std::vector<future<int>> channels2 = {std::move(f)};
        channels2.push_back(f);
        channels2.emplace_back(f);
#endif
        // here everything is OK
        channels[0] = std::move(f);
        channels.push_back(std::move(f));
        channels.emplace_back(std::move(f));
    }
    {
        auto f = make_ready_future(321);
        assert(f.is_ready());
        assert(f.get() == 321);
    }
    #if 0 // here it 'works' in Asan but it's broken in when_all test
    {
        auto f = make_ready_future__broken(123);
        assert(f.is_ready());
        assert(f.get() == 123);
    }
    {
        std::vector<future<int>> channels;
        auto i = 123;
        channels.emplace_back(std::move(make_ready_future__broken(std::move(i)))); // <--- move because it's NOT r-value!!
    }
    #endif
    {
        std::vector<future<int>> channels;
        for (auto i = 0; i < 4; i++) {
            channels.emplace_back(std::move(make_ready_future(std::move(i))));
        }
        auto result = when_all(std::move(channels)).then([](auto channels_) mutable {
            return 123;
        });
        assert(result.get() == 123);
    }
    {
        std::vector<future<int>> channels;
        for (auto i = 0; i < 4; i++) {
            auto f = execution::require(pool.executor(), execution::twoway).twoway_execute([i]{
                return i;
            });
            channels.emplace_back(std::move(f));
        }
        auto result = when_all(std::move(channels)).then([](auto channels_) mutable {
            return 123;
        });
        assert(result.get() == 123);
    }
    std::cout << "Tests: OK\n\n";
}

static auto one_leader_one_replica_scenario_with_consensus() {
    std::array replicas = {Replica(true)};
    auto leader = Leader(replicas, 123);
    execution::require(pool.executor(), execution::oneway).execute([&replicas]() mutable {
        replicas[0].run();
    });
    execution::require(pool.executor(), execution::oneway).execute([&leader]{
        leader.run();
    });
    pool.wait();
}

#if 0
static auto one_leader_more_replicas_scenario_no_consensus() {
    auto reps = std::vector<Replica>{Replica(true), Replica(false), Replica(true)};
    auto leader = Leader(reps, 123);
    execution::require(pool.executor(), execution::oneway).oneway_execute([replica = std::move(replica)]{
        boost::range::for_each(reps, [](auto &&rep){ rep.run(); });
    });
    execution::require(pool.executor(), execution::oneway).oneway_execute([leader = std::move(leader)]{
        leader.run();
    });
}
#endif

int main() {
    basic_tests();
    one_leader_one_replica_scenario_with_consensus();
    return 0;
}
