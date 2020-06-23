#pragma once

#include <vector>
#include <sys/types.h>
#include <sys/time.h>
#include <ucontext.h>
#include <signal.h>
#include <time.h>
#include <cassert>
#include <unistd.h>
#include <fmt/core.h>

class cooperative_scheduler final {
public:
    template<class... Args>
    cooperative_scheduler(Args&&... args) noexcept
        : scheduler_stack(std::malloc(stack_size))
    {
        constexpr auto contexts_number = (sizeof...(args))/2u;
        assert(contexts_number > 0u);
        just_me = this;
        assert(scheduler_stack);
        make_contexts(std::forward<Args>(args)...);
        setup_signals();
        setup_timer();
        assert(contexts.size() == contexts_number);
        init_scheduler_context();
        prefix_size = just_me->contexts.size();
        auto rc = swapcontext(&main_context, &contexts[current_context]);
        assert(rc >= 0);
    }
    cooperative_scheduler(const cooperative_scheduler&) = delete;
    cooperative_scheduler& operator=(const cooperative_scheduler&) = delete;
    ~cooperative_scheduler() {
        if constexpr (debug) {
            fmt::print("start cleanup\n");
        }
        for (auto i = 0u; i < just_me->contexts.size(); i++) {
            auto &context = just_me->contexts[i];
            std::free(context.uc_stack.ss_sp);
        }
        std::free(just_me->scheduler_stack);
    }
private:
    template<class Arg>
    void make_contexts(void (*fiber) (Arg*), Arg &arg) noexcept {
        make_context(fiber, &arg);
    }

    template<class Arg, class... Args>
    void make_contexts(void (*fiber) (Arg*), Arg &arg, Args&&... args) noexcept {
        make_context(fiber, &arg);
        make_contexts(std::forward<Args>(args)...);
    }

    static void round_rubin_scheduler() noexcept {
        auto &prefix_size = just_me->prefix_size;
        auto old_context = just_me->current_context;
        if (!by_interrupt) {
            // it means that old_context finished via uc_link
            std::swap(just_me->contexts[old_context], just_me->contexts[prefix_size-1]);
            prefix_size--;
        }
        by_interrupt = false;
        assert(!just_me->contexts.empty());
        if (prefix_size > 0) {
            just_me->current_context = (just_me->current_context + 1) % prefix_size;
            if constexpr (debug) {
                fmt::print("scheduling: fiber {} -> fiber {}\n", old_context, just_me->current_context);
            }
            auto ptr = &(just_me->contexts[just_me->current_context]);
            setcontext(ptr);
        }
    }

    static void scheduler_interrupt(int, siginfo_t*, void*) noexcept {
        just_me->init_scheduler_context();
        // save running thread, jump to scheduler
        auto ptr = &(just_me->contexts[just_me->current_context]);
        by_interrupt = true;
        auto rc = swapcontext(ptr, &(just_me->scheduler_context));
        assert(rc >= 0);
    }

    void setup_signals() noexcept {
        struct sigaction action;
        action.sa_sigaction = scheduler_interrupt;
        sigemptyset(&action.sa_mask);
        action.sa_flags = SA_RESTART | SA_SIGINFO;

        sigemptyset(&signal_mask_set);
        sigaddset(&signal_mask_set, SIGALRM);
        auto rc = sigaction(SIGALRM, &action, nullptr);
        assert(rc == 0);
    }

    void init_scheduler_context() noexcept {
        auto scheduler_context = &(just_me->scheduler_context);
        auto rc = getcontext(scheduler_context);
        assert(rc == 0);
        scheduler_context->uc_stack =  {just_me->scheduler_stack, 0, stack_size};
        scheduler_context->uc_link = &just_me->main_context;
        sigemptyset(&(scheduler_context->uc_sigmask));
        makecontext(scheduler_context, round_rubin_scheduler, 1);
    }

    template<class Arg>
    void make_context(void (*fiber) (Arg*), Arg *a) noexcept {
        contexts.emplace_back();
        auto uc = &contexts.back();
        auto rc = getcontext(uc);
        assert(rc == 0);
        auto stack = std::malloc(stack_size);
        assert(stack);
        uc->uc_stack = {stack, 0, stack_size};
        uc->uc_link = &scheduler_context;
        rc = sigemptyset(&uc->uc_sigmask);
        assert(rc >= 0);
        auto ptr = reinterpret_cast<void (*)(void)>(fiber);
        makecontext(uc, ptr, 1, a);
        if constexpr (debug) {
            fmt::print("context: {}\n", static_cast<void*>(uc));
        }
    }

    static void setup_timer() noexcept {
        timeval tv = {0, timer_us};
        itimerval it = {tv, tv};
        auto rc = setitimer(ITIMER_REAL, &it, nullptr);
        assert(rc == 0);
    }

    constexpr static auto stack_size = 16'384u;
    constexpr static auto debug = true;
    constexpr static auto timer_us = (debug)? 100'000 : 1'000; // 100ms or 1ms
    static cooperative_scheduler* just_me;
    sigset_t signal_mask_set;
    // used only in scheduler_interrupt, no need for volatile
    ucontext_t scheduler_context;
    // global interrupt stack
    void *scheduler_stack;
    std::vector<ucontext_t> contexts;
    ucontext_t main_context;
    unsigned current_context = 0u;
    unsigned prefix_size;
    static volatile bool by_interrupt;
};

cooperative_scheduler* cooperative_scheduler::just_me = nullptr;
volatile bool cooperative_scheduler::by_interrupt = false;
