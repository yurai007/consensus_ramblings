
#include "cooperative_scheduler.hh"
#include <fmt/core.h>
#include <future>
#include <coroutine>

template <typename R, typename... Args>
struct std::coroutine_traits<std::future<R>, Args...> {
    // promise_type - part of coroutine state
    struct promise_type {
        std::promise<R> p;
        suspend_never initial_suspend() { return {}; }
        suspend_never final_suspend() { return {}; }
        void return_value(R v) {
            p.set_value(v);
        }
        // cannot have simultanuelsy return_void with return_value
        //void return_void() {}
        std::future<R> get_return_object() { return p.get_future(); }
        void unhandled_exception() { p.set_exception(std::current_exception()); }
    };
};

namespace naive {

static void fiber0(int *p) {
    fmt::print("fiber0: started\n");
    sleep(5);
    fmt::print("fiber0: before sleep\n");
    sleep(5);
    fmt::print("fiber0: {}\n", *p);
    fmt::print("fiber0: returning\n");
}

static void fiber1(int *p) {
    fmt::print("fiber1: started\n");
    sleep(2);
    sleep(5);
    fmt::print("fiber1: after sleep\n");
    fmt::print("fiber1: {}\n", *p);
    fmt::print("fiber1: returning\n");
}

void test() {
   int p1 = 123, p2 = 321;
   cooperative_scheduler{fiber0, p1, fiber1, p2};
   fmt::print("unreachable end of scope\n");
}
}

namespace with_proper_cleanup {

std::promise<bool> done;
std::future<bool> donef = done.get_future();

static void fiber0(int *p) {
    auto task = [p]() -> std::future<int> {
        fmt::print("fiber0: started\n");
        auto n = sleep(5);
        fmt::print("fiber0: before 2nd sleep {}s\n", n);
        sleep(5);
        fmt::print("fiber0: {}\n", *p);
        fmt::print("fiber0: returning\n");
        co_return 0;
    };
    task().get();
    donef.get();
}

static void fiber1(int *p) {
    auto task = [p]() -> std::future<int> {
        fmt::print("fiber1: started\n");
        auto n = sleep(2);
        n = sleep(5);
        fmt::print("fiber1: after sleep {}s\n", n);
        fmt::print("fiber1: {}\n", *p);
        fmt::print("fiber1: returning\n");
        co_return 0;
    };
    task().get();
    done.set_value(true);
}

void test() {
   int p1 = 123, p2 = 321;
   cooperative_scheduler{fiber0, p1, fiber1, p2};
   fmt::print("unreachable end of scope\n");
}
}

int main() {
    //naive::test();
    with_proper_cleanup::test();
    return 0;
}
