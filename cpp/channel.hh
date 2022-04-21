#pragma once
#include <fmt/core.h>
#include <future>
#include <cassert>
#include <signal.h>
#include <time.h>
#include <concepts>
#include <optional>
#include <algorithm>

#ifdef __clang__
    #include <experimental/coroutine>
    #include <boost/range/algorithm/for_each.hpp>
    namespace stdx = std::experimental;
    namespace ranges = boost;
#else
    #include <coroutine>
    namespace stdx = std;
    namespace ranges = std::ranges;
#endif

namespace internal {

template<class T>
concept Awaiter = requires(T &t) {
    {t.await_ready() } -> std::same_as<bool>;
    {t.await_suspend(stdx::coroutine_handle<>{}) } -> std::same_as<void>;
    {t.await_resume() } -> std::same_as<bool>;
};

template <template <class U> class Channel, class T, class Writer, class Reader>
concept CoroutineChannelAux = requires (Channel<T>& c, T &t, Writer &w, Reader &r) {
    {c.write(t)} noexcept -> std::same_as<Writer>;
    {c.read()} noexcept -> std::same_as<Reader>;
    Awaiter<Writer>;
    Awaiter<Reader>;
};
}

template< template <class U> class Channel, class T>
concept CoroutineChannel = requires (Channel<T> &c) {
    internal::CoroutineChannelAux<Channel, typename Channel<T>::value_type,
        typename Channel<T>::Writer, typename Channel<T>::Reader>;
};

template <typename R, typename... Args>
struct stdx::coroutine_traits<std::future<R>, Args...> {
    // promise_type - part of coroutine state
    struct promise_type {
        std::promise<R> p;
        suspend_never initial_suspend() { return {}; }
        suspend_never final_suspend() noexcept { return {}; }
        void return_value(R v) {
            p.set_value(v);
        }
        // cannot have simultanuelsy return_void with return_value
        //void return_void() {}
        std::future<R> get_return_object() { return p.get_future(); }
        void unhandled_exception() { p.set_exception(std::current_exception()); }
    };
};

// A non-null address that leads access violation
static inline void* poison() noexcept {
    return reinterpret_cast<void*>(0xFADE'038C'BCFA'9E64);
}

bool debug = true;

template <typename T>
class list {
public:
    list() noexcept = default;

    bool is_empty() const noexcept {
        return head == nullptr;
    }
    void push(T* node) noexcept {
        if (tail) {
            tail->next = node;
            tail = node;
        } else
            head = tail = node;
    }
    T* pop() noexcept {
        auto node = head;
        if (head == tail)
            head = tail = nullptr;
        else
            head = head->next;
        return node;
    }
private:
    T* head{};
    T* tail{};
};

template <typename T>
class channel;
template <typename T>
class reader;
template <typename T>
class writer;

struct AsyncTimer {
    static void timer(int, siginfo_t* si, void*) noexcept {
        timer_t *tidp = reinterpret_cast<timer_t*>(si->si_value.sival_ptr);
        if (debug) {
            fmt::print("handler timerid = {}\n", *tidp);
        }
        auto real_frame =  (void*)(timerIdToFrame[reinterpret_cast<intptr_t>(*tidp)]);
        // FIXME: problem when coroutine is dead
        if (auto coro = stdx::coroutine_handle<>::from_address(real_frame)) {
            coro.resume();
        }
    }

   void setup_signals(void *ptr) noexcept {
       struct sigaction sa;
       sa.sa_flags = SA_SIGINFO;
       sa.sa_sigaction = timer;
       sigemptyset(&sa.sa_mask);
       assert(sigaction(SIGRTMIN, &sa, nullptr) != -1);

       sigevent sev;
       sev.sigev_notify = SIGEV_SIGNAL;
       sev.sigev_signo = SIGRTMIN;

       sev.sigev_value.sival_ptr = &timerid;
       assert(timer_create(CLOCK_REALTIME, &sev, &timerid) != -1);
       timerIdToFrame.push_back(ptr);
       if (debug) {
            fmt::print("setup signal for timerid = {}\n", timerid);
       }
    }

   void setup_timer(long timer_ns) noexcept {
       itimerspec its;
       its.it_value.tv_sec = timer_ns / 1'000'000'000;
       its.it_value.tv_nsec = timer_ns % 1'000'000'000;
       its.it_interval.tv_sec = its.it_value.tv_sec;
       its.it_interval.tv_nsec = its.it_value.tv_nsec;
       assert(timer_settime(timerid, 0, &its, NULL) != -1);
       if (debug) {
           fmt::print("setup timer = {} for {} ms \n", timerid, timer_ns/1'000'000);
       }
   }
   // FIXME: volatile
   static std::vector<void*> timerIdToFrame;
   timer_t timerid;
};
std::vector<void*> AsyncTimer::timerIdToFrame = {};

template <typename T>
class [[nodiscard]] reader final {
public:
    using value_type = T;
    using pointer = T*;
    using reference = T&;
    using channel_type = channel<T>;

private:
    using reader_list = typename channel_type::reader_list;
    using writer = typename channel_type::Writer;
    using writer_list = typename channel_type::writer_list;

    friend channel_type;
    friend writer;
    friend reader_list;

protected:
    mutable pointer value_ptr;
    mutable void* frame; // Resumeable Handle
    union {
        reader* next = nullptr; // Next reader in channel
        channel_type* channel_;     // Channel to push this reader
    };
    unsigned timeout_ms;
    std::optional<AsyncTimer> maybe_timer;
    mutable bool writer_ready = false;

private:
    explicit reader(channel_type& ch, unsigned _timeout_ms) noexcept
        : value_ptr{}, frame{}, channel_{std::addressof(ch)}, timeout_ms(_timeout_ms) {
    }
    reader(const reader&) noexcept = delete;
    reader& operator=(const reader&) noexcept = delete;

public:
    reader(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
        std::swap(maybe_timer, rhs.maybe_timer);
    }
    reader& operator=(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
        std::swap(maybe_timer, rhs.maybe_timer);
        return *this;
    }
    ~reader() noexcept = default;

public:
    bool await_ready() const  {
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        if (channel_->writer_list::is_empty()) {
            return false;
        }
        auto w = channel_->writer_list::pop();
        // exchange address & resumeable_handle
        std::swap(value_ptr, w->value_ptr);
        std::swap(frame, w->frame);
        writer_ready = true;
        return true;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        if (debug) {
            fmt::print("{} with timeout = {}ms\n", __PRETTY_FUNCTION__, timeout_ms);
        }
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel_);
        frame = coro.address(); // remember handle before push
        next = nullptr;         // clear to prevent confusing

        ch.reader_list::push(this);
        init_timer(frame);
    }
    std::tuple<value_type, bool> await_resume() {
        // WTF??
        // fmt::print("{} {}\n", __PRETTY_FUNCTION__);
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        if (maybe_timer) {
            maybe_timer->setup_timer(0);
        }
        auto t = std::make_tuple(value_type{}, false);
        // frame holds poision if the channel is going to be destroyed
        if (frame == poison())
            return t;
        // Store first. we have to do this because the resume operation
        // can destroy the writer coroutine
        auto& value = std::get<0>(t);
        if (value_ptr) {
            value = std::move(*value_ptr);
        }
        if (debug) {
            if constexpr(std::is_pointer<T>::value) {
                fmt::print(" T = {}\n", typeid(*value).name());
            } else {
                fmt::print("\n");
            }
        }
        // assuming there is writer
        if (writer_ready) {
            if (auto coro = stdx::coroutine_handle<>::from_address(frame))
                coro.resume();
        }
        std::get<1>(t) = true;
        return t;
    }
private:
    void init_timer(void *ptr) {
        if (timeout_ms) {
            if (debug) {
                fmt::print("{}", __PRETTY_FUNCTION__);
            }
            maybe_timer = AsyncTimer();
            // set one-shot timer which fire await_resume after ms
            maybe_timer->setup_signals(ptr);
            maybe_timer->setup_timer(timeout_ms * 1'000'000);
        }
    }
};

template <typename T>
class [[nodiscard]] writer final {
public:
    using value_type = T;
    using pointer = T*;
    using reference = T&;
    using channel_type = channel<T>;

private:
    using reader = typename channel_type::Reader;
    using reader_list = typename channel_type::reader_list;
    using writer_list = typename channel_type::writer_list;

    friend channel_type;
    friend reader;
    friend writer_list;

private:
    mutable pointer value_ptr;
    mutable void* frame; // Resumeable Handle
    union {
        writer* next = nullptr; // Next writer in channel
        channel_type* channel_;     // Channel to push this writer
    };

private:
    explicit writer(channel_type& ch, const pointer pv) noexcept
        : value_ptr{pv}, frame{}, channel_{std::addressof(ch)} {
    }
    writer(const writer&) noexcept = delete;
    writer& operator=(const writer&) noexcept = delete;

public:
    writer(writer&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
    }
    writer& operator=(writer&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
        return *this;
    }
    ~writer() noexcept = default;

public:
    bool await_ready() const {
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        if (channel_->reader_list::is_empty()) {
            return false;
        }
        auto r = channel_->reader_list::pop();
        // exchange address & resumeable_handle
        std::swap(value_ptr, r->value_ptr);
        std::swap(frame, r->frame);
        return true;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel_);

        frame = coro.address(); // remember handle before push
        next = nullptr;         // clear to prevent confusing

        ch.writer_list::push(this);
    }
    bool await_resume() {
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        // frame holds poision if the channel is going to destroy
        if (frame == poison()) {
            fmt::print("poison\n");
            return false;
        }
        if (auto coro = stdx::coroutine_handle<>::from_address(frame)) {
            coro.resume();
        }
        return true;
    }
};

template <typename T>
class channel final : list<reader<T>>, list<writer<T>> {
    static_assert(std::is_reference<T>::value == false,
                  "reference type can't be channel's value_type.");

public:
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;
    using Reader = reader<value_type>;
    using Writer = writer<value_type>;
private:
    using reader_list = list<Reader>;
    using writer_list = list<Writer>;

    friend Reader;
    friend Writer;
public:
    channel(const channel&) noexcept = delete;
    channel(channel&&) noexcept = delete;
    channel& operator=(const channel&) noexcept = delete;
    channel& operator=(channel&&) noexcept = delete;
    channel() noexcept : reader_list{}, writer_list{} {}

    // noexcept(false) destructor is PITA - it's popagated to all users.
    // Make it noexcept
    ~channel()
    {
        if (debug) {
            fmt::print("{}\n", __PRETTY_FUNCTION__);
        }
        writer_list& writers = *this;
        reader_list& readers = *this;
        //
        // If the channel is raced hardly, some coroutines can be
        //  enqueued into list just after this destructor unlocks mutex.
        //
        // Unfortunately, this can't be detected at once since
        //  we have 2 list (readers/writers) in the channel.
        //
        // Current implementation allows checking repeatedly to reduce the
        //  probability of such interleaving.
        // Increase the repeat count below if the situation occurs.
        // But notice that it is NOT zero.
        //
        auto repeat = 1; // author experienced 5'000+ for hazard usage
        while (repeat--) {
            while (!writers.is_empty()) {
                auto w = writers.pop();
                auto coro = stdx::coroutine_handle<>::from_address(w->frame);
                w->frame = poison();

                coro.resume();
            }
            while (!readers.is_empty()) {
                auto r = readers.pop();
                auto coro = stdx::coroutine_handle<>::from_address(r->frame);
                r->frame = poison();

                coro.resume();
            }
        }
    }

public:
    Writer write(reference ref) noexcept {
        return Writer{*this, std::addressof(ref)};
    }
    Reader read() noexcept {
        return Reader{*this, 0};
    }
    Reader readWithTimeout(unsigned timeoutMs) noexcept {
        return Reader{*this, timeoutMs};
    }
};

struct [[nodiscard]] DummyAwaitable {
    unsigned ms;
    void* frame = nullptr;
    AsyncTimer timer;
    static constexpr bool ready = false;

    DummyAwaitable(unsigned _ms)
    : ms(_ms) {}

    bool await_ready() {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        return ready;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        frame = coro.address();
        // set one-shot timer which fire await_resume after ms
        timer.setup_signals(frame);
        timer.setup_timer(ms * 1'000'000);
    }
    bool await_resume() {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        timer.setup_timer(0);
        if (frame) {
            if (auto coro = stdx::coroutine_handle<>::from_address(frame)) {
                return true;
            }
        }
        return false;
    }
};

DummyAwaitable delay(unsigned time_ms) noexcept {
    return DummyAwaitable{time_ms};
}
