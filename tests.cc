#include "utils.hh"
#include <vector>
#include <cassert>
#include <iostream>
#include <type_traits>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/iterator_range.hpp>

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
            channels.emplace_back(make_ready_future(std::move(i)));
        }
        auto result = when_all(std::move(channels)).then([](auto) mutable noexcept {
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
        auto result = when_all(std::move(channels)).then([](auto) mutable noexcept {
            return 123;
        });
        assert(result.get() == 123);
    }
    {
        using namespace boost::adaptors;
        auto integers = {0, 1, 2, 3};
        // extra copy from range to collection needed
        auto result = when_all(boost::copy_range<std::vector<future<int>>>(
                                   integers | transformed([] (auto i) { return make_ready_future(std::move(i)); })
                               ));
        auto expected = 0;
        for (auto &&i : result.get()) {
            assert(i.get() == expected++);
        }
    }
    pool.wait();
    std::cout << "Tests: OK\n\n";
}


#define FWD(...) ::std::forward<decltype(__VA_ARGS__)>(__VA_ARGS__)

// basic concepts from C++20 <concepts>
#ifndef __clang__
template <class T>
concept bool Destructible = std::is_nothrow_destructible_v<T>;

template <class T, class... Args>
concept bool Constructible = Destructible<T> && std::is_constructible_v<T, Args...>;

template <class From, class To>
concept bool ConvertibleTo =
        std::is_convertible_v<From, To> &&
requires(From (&f)()) {
    static_cast<To>(f());
};

template<class T>
concept bool MoveConstructible = Constructible<T, T> && ConvertibleTo<T, T>;

template <class T>
concept bool CopyConstructible = Constructible<T> && MoveConstructible<T>;
#endif

template <class Executor, class Function>
#ifndef __clang__
requires requires(Executor e, Function f) {
   // CopyConstructible<Executor>;
    e.execute(f);
}
#endif
inline void async1(Executor &ex, Function &&f) {
    execution::require(ex, execution::single, execution::oneway).execute(FWD(f));
}

template <class Executor, class Function>
#ifndef __clang__
#endif
inline auto async(Executor ex, Function &&f) {
    return execution::require(ex, execution::twoway).twoway_execute(FWD(f));
}

class inline_executor {
public:
#if 0
    inline_executor() = default;
    inline_executor(const inline_executor&) = delete;
    inline_executor &operator=(inline_executor&) = delete;
#endif
    friend bool operator==(const inline_executor&, const inline_executor&) noexcept {
        return true;
    }

    friend bool operator!=(const inline_executor&, const inline_executor&) noexcept {
        return false;
    }
// here concept in async1 helps a lot
#if 0
#endif
    template <class Function>
    inline void execute(Function &&f) const noexcept {
        f();
    }
};

static auto executors_tests() {
    static_thread_pool pool(1);
    // graceful shutdown (joining in wait())
    {
        static_thread_pool _pool(1);
        auto agent = execution::require(_pool.executor(), execution::single, execution::oneway);
        auto done = false;
        agent.execute([&done]{
            done = true;
        });
        _pool.wait();
        assert(done);
    }
    // force shutdown (no joins)
    {
        static_thread_pool _pool(1);
        auto agent = execution::require(_pool.executor(), execution::single, execution::oneway);
        auto done = false;
        agent.execute([&done]{
            done = true;
        });
        _pool.stop();
        assert(!done);
    }
    {
        auto agent = execution::require(pool.executor(), execution::twoway);
        auto f = agent.twoway_execute([]{});
        assert(f.valid());
        f.get();
        assert(!f.valid());
    }
    {
        auto f = execution::require(pool.executor(), execution::oneway).twoway_execute([](){
            return 42;
        });
        assert(f.get() == 42);
    }
#if 0
    {
        static_assert(execution::is_oneway_executor_v<inline_executor>);
        inline_executor executor;
        auto agent = execution::require(executor, execution::oneway, execution::single);
        auto done = false;
        agent.execute([&done]{
            done = true;
        });
        assert(done);
    }
#endif
    {
        static_assert(execution::is_oneway_executor_v<inline_executor>);
        inline_executor executor;
        auto done = false;
        async1(executor, [&done]{
            done = true;
        });
        assert(done);
    }
    {
        auto f = execution::require(pool.executor(), execution::twoway).twoway_execute([](){
            return 42;
        }).then([](auto maybe_value){
            auto value = maybe_value.get();
            return ++value;
        }).then([](auto maybe_value){
            auto value = maybe_value.get();
            return ++value;
        });
        assert(f.get() == 44);
    }
    pool.wait();
}

int main() {
    basic_tests();
    executors_tests();
    return 0;
}
