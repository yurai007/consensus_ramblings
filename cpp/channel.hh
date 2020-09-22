#pragma once
#include <fmt/core.h>
#include <mutex>
#include <future>

#ifdef __clang__
    #include <experimental/coroutine>
    namespace stdx = std::experimental;
#else
    #include <coroutine>
    namespace stdx = std;
#endif

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

template <typename T>
class channel;
template <typename T>
class reader;
template <typename T>
class writer;

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

private:
    explicit reader(channel_type& ch) noexcept
        : value_ptr{}, frame{}, channel_{std::addressof(ch)} {
    }
    reader(const reader&) noexcept = delete;
    reader& operator=(const reader&) noexcept = delete;

public:
    reader(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
    }
    reader& operator=(reader&& rhs) noexcept {
        std::swap(value_ptr, rhs.value_ptr);
        std::swap(frame, rhs.frame);
        std::swap(channel_, rhs.channel_);
        return *this;
    }
    ~reader() noexcept = default;

public:
    bool await_ready() const  {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        if (channel_->writer_list::is_empty()) {
            return false;
        }
        auto w = channel_->writer_list::pop();
        // exchange address & resumeable_handle
        std::swap(value_ptr, w->value_ptr);
        std::swap(frame, w->frame);
        return true;
    }
    void await_suspend(stdx::coroutine_handle<> coro) {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel_);
        frame = coro.address(); // remember handle before push/unlock
        next = nullptr;         // clear to prevent confusing

        ch.reader_list::push(this);
    }
    std::tuple<value_type, bool> await_resume() {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
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
    explicit writer(channel_type& ch, pointer pv) noexcept
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
        fmt::print("{}\n", __PRETTY_FUNCTION__);
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
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        // notice that next & chan are sharing memory
        auto& ch = *(this->channel_);

        frame = coro.address(); // remember handle before push/unlock
        next = nullptr;         // clear to prevent confusing

        ch.writer_list::push(this);
    }
    bool await_resume() {
        fmt::print("{}\n", __PRETTY_FUNCTION__);
        // frame holds poision if the channel is going to destroy
        if (frame == poison()) {
            return false;
        }
        if (auto coro = stdx::coroutine_handle<>::from_address(frame))
            coro.resume();

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
        reader_list& readers = *this;
        return Reader{*this};
    }
};
