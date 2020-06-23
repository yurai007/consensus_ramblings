#include "cooperative_scheduler.hh"
#include <fmt/core.h>
#include <future>
#ifdef __clang__
    #include <experimental/coroutine>
    namespace stdx = std::experimental;
#else
    #include <coroutine>
    namespace stdx = std;
#endif

// expplicit cons sched
template <typename R, typename... Args>
struct stdx::coroutine_traits<std::future<R>, Args...> {
    // promise_type - part of coroutine state
    struct promise_type {
        std::promise<R> p;
        suspend_never initial_suspend() { return {}; }
        suspend_never final_suspend() { return {}; }
        void return_value(R v) {
            p.set_value(v);
        }
        std::future<R> get_return_object() { return p.get_future(); }
        void unhandled_exception() { p.set_exception(std::current_exception()); }
    };
};

void fiber0(int *) {
    fmt::print("func0: started\n");
}

void fiber00(int *) {
    fmt::print("func00: started\n");
}

void fiber1(int *p) {
    fmt::print("fiber0: started\n");
    sleep(5);
    fmt::print("fiber0: before sleep\n");
    sleep(5);
    fmt::print("fiber0: {}\n", *p);
    fmt::print("fiber0: returning\n");
}

void fiber2(int *p) {
    fmt::print("fiber1: started\n");
    sleep(2);
    sleep(5);
    fmt::print("fiber1: after sleep\n");
    fmt::print("fiber1: {}\n", *p);
    fmt::print("fiber1: returning\n");
}

void test0() {
    int p1 = 123;
    cooperative_scheduler{fiber0, p1};
    fmt::print("end of scope\n\n");
}

void test1() {
    int p1 = 123, p2 =32;
    cooperative_scheduler{fiber0, p1, fiber00, p2};
    fmt::print("end of scope\n\n");
}

void test2() {
   int p1 = 123, p2 = 321;
   cooperative_scheduler{fiber1, p1, fiber2, p2};
   fmt::print("end of scope\n\n");
}

void test3() {
   int p1 = 123, p2 = 321, p3 = 3;
   cooperative_scheduler{fiber00, p3, fiber1, p1, fiber2, p2};
   fmt::print("end of scope\n\n");
}

// those hacks are not needed anymore
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
   fmt::print("end of scope\n\n");
}
}

int main() {
    test0();
    test1();
    test2();
    test3();
    with_proper_cleanup::test();
    return 0;
}
