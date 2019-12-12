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
    cooperative_scheduler(void (*f1) (void), void (*f2) (void)) noexcept {
        just_me = this;
        std::cout << "pid: " << getpid() << std::endl;
        // allocate the global signal/interrupt stack
        signal_stack = malloc(stacksize);
        assert(signal_stack);

        mkcontext(f2);
        mkcontext(f1);
        // initialize the signal handlers
        setup_signals();
        setup_timer();
        auto rc = std::atexit([]{
            // temporary workaround to silent memcheck
            // we need to be sure all fibers done their job
            std::cout << "cleanup" << std::endl;
            for (auto &&context : just_me->contexts) {
                free(context.uc_stack.ss_sp);
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
    static void timer_interrupt(int, siginfo_t *, void *) noexcept {
        // Create new scheduler context
        auto signal_context = &(just_me->signal_context);
        getcontext(signal_context);
        signal_context->uc_stack.ss_sp = just_me->signal_stack;
        signal_context->uc_stack.ss_size = stacksize;
        signal_context->uc_stack.ss_flags = 0;
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
    void mkcontext(void (*function) (void)) noexcept {
        contexts.emplace_back();
        auto *uc = &contexts.back();
        getcontext(uc);

        auto *stack = malloc(stacksize);
        assert(stack);
        // we need to initialize the ucontext structure, give it a stack flags, and a sigmask
        uc->uc_stack.ss_sp = stack;
        uc->uc_stack.ss_size = stacksize;
        uc->uc_stack.ss_flags = 0;
        auto rc = sigemptyset(&uc->uc_sigmask);
        assert(rc >= 0);
        makecontext(uc, function, 1);
        std::cout << "context is " << uc << std::endl;
    }

    static void setup_timer() noexcept {
        itimerval it;
        it.it_interval.tv_sec = 0;
        it.it_interval.tv_usec = 100'000; // 100 ms
        it.it_value = it.it_interval;
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
