#include "utils.hh"
#include <vector>
#include <cassert>
#include <iostream>
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
    std::cout << "Tests: OK\n\n";
}

int main() {
    basic_tests();
    return 0;
}
