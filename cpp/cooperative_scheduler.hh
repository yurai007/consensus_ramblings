#pragma once

#include <vector>
#include <sys/types.h>
#include <sys/time.h>
#include <ucontext.h>
#include <signal.h>
#include <time.h>
#include <cassert>
#include <unistd.h>
#include <iostream>

class cooperative_scheduler final {
public:
    template<class... Args>
    cooperative_scheduler(Args&&... args) noexcept {
        just_me = this;
        std::cout << "pid: " << getpid() << std::endl;
        signal_stack = std::malloc(stacksize);
        assert(signal_stack);
        make_contexts(std::forward<Args>(args)...);
        setup_signals();
        setup_timer();
        auto rc = std::atexit([]{
            // temporary workaround to silent memcheck
            // we need to be sure all fibers done their job
            std::cout << "cleanup" << std::endl;
            for (auto i = 0u; i < just_me->contexts.size(); i++) {
                auto &context = just_me->contexts[i];
                std::free(context.uc_stack.ss_sp);
            }
            free(just_me->signal_stack);
            just_me->~cooperative_scheduler();
        });
        assert(rc == 0);
        assert(!contexts.empty());
        setcontext(&contexts[current_context]);
    }
    cooperative_scheduler(const cooperative_scheduler&) = delete;
    cooperative_scheduler& operator=(const cooperative_scheduler&) = delete;
private:
    template<class Arg>
    void make_contexts(void (*f) (Arg*), Arg &a) noexcept {
        mkcontext(f, &a);
    }

    template<class Arg, class... Args>
    void make_contexts(void (*f) (Arg*), Arg &a, Args&&... args) noexcept {
        mkcontext(f, &a);
        make_contexts(std::forward<Args>(args)...);
    }

    static void round_rubin_scheduler() noexcept {
        assert(!just_me->contexts.empty());
        auto old_context = just_me->current_context;
        just_me->current_context = (just_me->current_context + 1) % just_me->contexts.size();
        std::cout << "scheduling: fiber "  << old_context << " -> fiber " << just_me->current_context << std::endl;
        auto ptr = &(just_me->contexts[just_me->current_context]);
        setcontext(ptr);
    }

    /*
      Timer interrupt handler.
      Creates a new context to run the scheduler in, masks signals, then swaps
      contexts saving the previously executing thread and jumping to the
      scheduler.
    */
    static void timer_interrupt(int, siginfo_t*, void*) noexcept {
        // Create new scheduler context
        auto signal_context = &(just_me->signal_context);
        getcontext(signal_context);
        signal_context->uc_stack =  {just_me->signal_stack, 0, stacksize};
        sigemptyset(&(signal_context->uc_sigmask));
        makecontext(signal_context, round_rubin_scheduler, 1);

        // save running thread, jump to scheduler
        auto ptr = &(just_me->contexts[just_me->current_context]);
        swapcontext(ptr, signal_context);
    }

    /* Set up SIGALRM signal handler */
    void setup_signals() noexcept {
        struct sigaction action;
        action.sa_sigaction = timer_interrupt;
        sigemptyset(&action.sa_mask);
        action.sa_flags = SA_RESTART | SA_SIGINFO;

        sigemptyset(&signal_mask_set);
        sigaddset(&signal_mask_set, SIGALRM);
        auto rc = sigaction(SIGALRM, &action, nullptr);
        assert(rc == 0);
    }

    /* helper function to create a context.
       initialize the context from the current context, setup the new
       stack, signal mask, and tell it which function to call.
    */
    template<class Arg>
    void mkcontext(void (*function) (Arg*), Arg *a) noexcept {
        contexts.emplace_back();
        auto uc = &contexts.back();
        getcontext(uc);
        auto stack = std::malloc(stacksize);
        assert(stack);
        uc->uc_stack = {stack, 0, stacksize};
        auto rc = sigemptyset(&uc->uc_sigmask);
        assert(rc >= 0);

        makecontext(uc, reinterpret_cast<void (*)(void)>(function), 1, a);
        std::cout << "context is " << uc << std::endl;
    }

    static void setup_timer() noexcept {
        timeval tv = {0, 100'000}; // 100ms
        itimerval it = {tv, tv};
        auto rc = setitimer(ITIMER_REAL, &it, nullptr);
        assert(rc == 0);
    }

    constexpr static auto stacksize = 16'384u;
    sigset_t signal_mask_set;
    // used only in timer_interrupt, no need for volatile
    ucontext_t signal_context;
    // global interrupt stack
    void *signal_stack;
    std::vector<ucontext_t> contexts;
    unsigned current_context = 0u;
    static cooperative_scheduler* just_me;
};

cooperative_scheduler* cooperative_scheduler::just_me = nullptr;
