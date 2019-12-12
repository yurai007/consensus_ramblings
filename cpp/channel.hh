#pragma once

#include <experimental/coroutine>
#include <mutex>
#include <iostream>
#include <future>

/* Ref: https://luncliff.github.io/coroutine/articles/designing-the-channel
   TO DO: hmm, even without iostream, still:
   ERROR SUMMARY: 4 errors from 4 contexts (suppressed: 68 from 38) from helgrind,
          but maybe because of static_thread_pool, check it later only with channel.

 channel = (writer_list(head, tail),
              reader_list(head, tail),
              mutex)
 * write(int x):
   - writer(channel, &x) -> channel() ->
        ptr = &x;
 * writer::await_ready()
 * co_await ch.write(msg) ->
        writer::await_suspend() -> remember coroutine_handle (frame) + writer_list::push()
 * co_await (msg, end) = ch.read() ->
        reader::await_ready() -> writer_list::pop(); + remember coroutine_handle (frame)
        from popped writer
 * reader::await_resume -> resume coroutine_handle (frame)

 Notice that:
 Our list is allocation free, we store there pointers to externally owned local values.

 TO DO: check destructor scenario
*/

namespace stdx = std::experimental;

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

struct bypass_lock final {
    constexpr bool try_lock() noexcept {
        return true;
    }
    constexpr void lock() noexcept {}
    constexpr void unlock() noexcept {}
};

template <typename T, typename M = std::mutex>
class channel;
template <typename T, typename M>
class reader;
template <typename T, typename M>
class writer;

template <typename T, typename M>
class [[nodiscard]] reader final {
public:
    using value_type = T;
    using pointer = T*;
    using reference = T&;
    using channel_type = channel<T, M>;

private:
    using reader_list = typename channel_type::reader_list;
    using writer = typename channel_type::writer;
    using writer_list = typename channel_type::writer_list;

    friend channel_type;
    friend writer;
    friend reader_list;

protected:
    mutable pointer value_ptr;
    mutable void* frame; // Resumeable Handle
    union {
        reader* next = nullptr; // Next reader in channel
        channel_type* channel;     // Channel to push this reader
    };

private:
    explicit reader(channel_type& ch) noexcept
        : value_ptr{}, frame{}, channel{std::addressof(ch)} {
    }
    reader(const reader&) noexcept = delete;
    reader& operator=(const reader&) noexcept = delete;

public:
    reader(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel, rhs.channel);
    }
    reader& operator=(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel, rhs.channel);
        return *this;
    }
    ~reader() noexcept = default;

public:
    bool await_ready() const  {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        channel->mtx.lock();
        if (channel->writer_list::is_empty()) {
            return false;
        }
        auto w = channel->writer_list::pop();
        // exchange address & resumeable_handle
        std::swap(value_ptr, w->value_ptr);
        std::swap(frame, w->frame);

        channel->mtx.unlock();
        return true;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel);
        frame = coro.address(); // remember handle before push/unlock
        next = nullptr;         // clear to prevent confusing

        ch.reader_list::push(this);
        ch.mtx.unlock();
    }
    std::tuple<value_type, bool> await_resume() {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        auto t = std::make_tuple(value_type{}, false);
        // frame holds poision if the channel is going to be destroyed
        if (frame == poison())
            return t;

        // Store first. we have to do this because the resume operation
        // can destroy the writer coroutine
        auto& value = std::get<0>(t);
        value = std::move(*value_ptr);
        if (auto coro = stdx::coroutine_handle<>::from_address(frame))
            coro.resume();

        std::get<1>(t) = true;
        return t;
    }
};

template <typename T, typename M>
class [[nodiscard]] writer final {
public:
    using value_type = T;
    using pointer = T*;
    using reference = T&;
    using channel_type = channel<T, M>;

private:
    using reader = typename channel_type::reader;
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
        channel_type* channel;     // Channel to push this writer
    };

private:
    explicit writer(channel_type& ch, pointer pv) noexcept
        : value_ptr{pv}, frame{}, channel{std::addressof(ch)} {
    }
    writer(const writer&) noexcept = delete;
    writer& operator=(const writer&) noexcept = delete;

public:
    writer(writer&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel, rhs.channel);
    }
    writer& operator=(writer&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel, rhs.channel);
        return *this;
    }
    ~writer() noexcept = default;

public:
    bool await_ready() const {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        channel->mtx.lock();
        if (channel->reader_list::is_empty()) {
            return false;
        }
        auto r = channel->reader_list::pop();
        // exchange address & resumeable_handle
        std::swap(value_ptr, r->value_ptr);
        std::swap(frame, r->frame);

        channel->mtx.unlock();
        return true;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel);

        frame = coro.address(); // remember handle before push/unlock
        next = nullptr;         // clear to prevent confusing

        ch.writer_list::push(this);
        ch.mtx.unlock();
    }
    bool await_resume() {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
        // frame holds poision if the channel is going to destroy
        if (frame == poison()) {
            return false;
        }
        if (auto coro = stdx::coroutine_handle<>::from_address(frame))
            coro.resume();

        return true;
    }
};

template <typename T, typename M>
class channel final : list<reader<T, M>>, list<writer<T, M>> {
    static_assert(std::is_reference<T>::value == false,
                  "reference type can't be channel's value_type.");

public:
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;
    using mutex_type = M;

private:
    using reader = reader<value_type, mutex_type>;
    using reader_list = list<reader>;
    using writer = writer<value_type, mutex_type>;
    using writer_list = list<writer>;

    friend reader;
    friend writer;

    mutex_type mtx;
public:
    channel(const channel&) noexcept = delete;
    channel(channel&&) noexcept = delete;
    channel& operator=(const channel&) noexcept = delete;
    channel& operator=(channel&&) noexcept = delete;
    channel() noexcept : reader_list{}, writer_list{}, mtx{} {}

    ~channel() noexcept(false)
    {
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
            std::unique_lock lck{mtx};

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
    writer write(reference ref) noexcept {
        return writer{*this, std::addressof(ref)};
    }
    reader read() noexcept {
        reader_list& readers = *this;
        return reader{*this};
    }
};
