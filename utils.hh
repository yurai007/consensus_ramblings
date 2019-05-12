#pragma once

#include <experimental/thread_pool>
#include <algorithm>

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

inline static_thread_pool pool(2);

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
#endif

template<class FuturesContainer>
#ifndef __clang__
requires requires (FuturesContainer c) {
    std::begin(c);
    std::end(c);
    Future<decltype(*std::begin(c))>;
}
#endif
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

