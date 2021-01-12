#include "cooperative_scheduler.hh"
#include "channel.hh"
#include <fmt/core.h>
#include <future>

#include <ucontext.h>
#include <sys/mman.h>
#include <valgrind/valgrind.h>

#ifdef __clang__
    #include <experimental/coroutine>
    namespace stdx = std::experimental;
#else
    #include <coroutine>
    namespace stdx = std;
#endif

    // those hacks are not needed anymore
    #if 0
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
    #endif

namespace scheduler {

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
    auto p1 = 123, p2 =32;
    cooperative_scheduler{fiber0, p1, fiber00, p2};
    fmt::print("end of scope\n\n");
}

void test2() {
   auto p1 = 123, p2 = 321;
   cooperative_scheduler{fiber1, p1, fiber2, p2};
   fmt::print("end of scope\n\n");
}

void test3() {
    {
        auto p1 = 123, p2 = 321, p3 = 3;
        cooperative_scheduler{fiber00, p3, fiber1, p1, fiber2, p2};
        fmt::print("end of scope\n\n");
    }
    // to make sure timer was disabled during cooperative_scheduler destruction
    sleep(3);
}

std::promise<bool> done;
std::future<bool> donef = done.get_future();

template<class T>
std::future<T> make_ready_future(T v) {
    std::promise<T> p;
    auto f = p.get_future();
    // it throws in libstdc++ if -lpthread is missing!
    p.set_value(v);
    return f;
}

void fiber3(int*) {
    auto task = []() -> std::future<int> {
        // blocking call is done immediately...
        fmt::print("before sleep\n");
        sleep(3);
        fmt::print("after sleep\n");
        return make_ready_future<int>(123);
    };
    assert(task().get() == 123);
    fmt::print("done set\n");
    done.set_value(true);
}

void fiber4(int*) {
    auto task = []() -> std::future<int> {
        return make_ready_future<int>(0);
    };
    auto t = task();
    fmt::print("check\n");
    // ...but here we are waiting
    assert(t.get() == 0);
    fmt::print("done get\n");
    donef.get();
    fmt::print("done ok\n");
}

static void test4() {
   int p1 = 123, p2 = 321;
   cooperative_scheduler{fiber3, p1, fiber4, p2};
   fmt::print("end of scope test4\n\n");
}

}

namespace channels {

channel<int> _channel1, _channel2;

std::future<int> one_fiber() {
    fmt::print("{}\n", __PRETTY_FUNCTION__);
    // fiber 1
    auto [rmsg, ok] = co_await _channel1.read();
    fmt::print("Read done  {}\n", rmsg);
    // Still fiber 1
    auto msg = 1;
    co_await _channel1.write(msg);
    fmt::print("Write done\n");
    co_return 0;
}

std::promise<bool> done;
std::future<bool> donef = done.get_future();

void fiber1(int*) {
    auto task = []() -> std::future<int> {
        fmt::print("fiber1: start\n");
        auto [msg, ok] = co_await _channel2.read();
        fmt::print("fiber1: end with {}\n", msg);
        co_return msg;
    };
    assert(task().get() == 123);
    done.set_value(true);
}

void fiber2(int*) {
    auto task = []() -> std::future<int> {
        fmt::print("fiber2: start\n");
        auto msg = 123;
        co_await _channel2.write(msg);
        fmt::print("fiber2: end\n");
        co_return 0;
    };
    auto t = task();
    fmt::print("check\n");
    assert(t.get() == 0);
    donef.get();
}

static void two_fibers() {
    fmt::print("{}\n", __PRETTY_FUNCTION__);
    auto i = 1, j = 2;
    cooperative_scheduler{fiber1, i, fiber2, j};
}
}

namespace channels_msgs {

struct Msg {
    unsigned x;
};

channel<std::unique_ptr<Msg>> rpc_channel;
std::promise<bool> done;
std::future<bool> donef = done.get_future();

static void producer(int*) {
    auto task = []() -> std::future<int> {
        fmt::print("producer: start\n");
        for (auto i = 1u; i <= 5u; i++) {
            auto msg = std::make_unique<Msg>(Msg{i*i});
            co_await rpc_channel.write(msg);
            fmt::print("producer wrote\n");
        }
        fmt::print("producer: end\n");
        co_return 0;
    };
    task().get();
    done.set_value(true);
}

static void consumer(int*) {
    auto task = []() -> std::future<int> {
        fmt::print("consumer: start\n");
        for (auto i = 1; i <= 5; i++) {
            auto [msg, ok] = co_await rpc_channel.read();
            fmt::print("consumer: {}\n", msg->x);
        }
        fmt::print("consumer: end\n");
        co_return 0;
    };
    task().get();
    donef.get();
}

static void test() {
     fmt::print("{}\n", __PRETTY_FUNCTION__);
     auto i = 1, j = 2;
     cooperative_scheduler{producer, i, consumer, j};
}

}

int main() {
    scheduler::test0();
    scheduler::test1();
    scheduler::test2();
    scheduler::test3();
    scheduler::test4();

    channels::one_fiber();
    channels::two_fibers();
    channels_msgs::test();
    return 0;
}
