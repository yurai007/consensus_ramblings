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
#include <valgrind/valgrind.h>
#include <boost/range/algorithm/for_each.hpp>

class cooperative_scheduler final {
public:
    template<class... Args>
    cooperative_scheduler(Args&&... args) noexcept
        : scheduler_stack(std::malloc(stack_size))
    {
        constexpr auto contexts_number = (sizeof...(args))/2u;
        assert(contexts_number > 0u);
        // to prevent contexts pointers invalidation
        contexts.reserve(contexts_number);
        make_contexts(std::forward<Args>(args)...);
        init(contexts_number);
    }
    template<class Fiber, class Arg>
    cooperative_scheduler(Fiber f, const std::vector<Arg> &args) noexcept
        : scheduler_stack(std::malloc(stack_size))
    {
        auto contexts_number = args.size();
        assert(contexts_number > 0u);
        // to prevent contexts pointers invalidation
        contexts.reserve(contexts_number);
        boost::for_each(args, [this, f](auto &arg){
            make_context(f, arg.get());
        });
        init(contexts_number);
    }
    cooperative_scheduler(const cooperative_scheduler&) = delete;
    cooperative_scheduler& operator=(const cooperative_scheduler&) = delete;
    ~cooperative_scheduler() {
        if (debug) {
            fmt::print("cleanup\n");
        }
        setup_timer(0);
        for (auto ms : memcheck_stacks) {
            VALGRIND_STACK_DEREGISTER(ms);
        }
        for (auto i = 0u; i < just_me->contexts.size(); i++) {
            auto &context = just_me->contexts[i];

            std::free(context.uc_stack.ss_sp);
        }
        std::free(just_me->scheduler_stack);
    }

     static bool debug;
private:
    void init(unsigned contexts_number) noexcept {
        just_me = this;
        assert(scheduler_stack);
        setup_signals();
        setup_timer(timer_us);
        assert(contexts.size() == contexts_number);
        init_scheduler_context();
        prefix_size = just_me->contexts.size();
        auto rc = swapcontext(&main_context, &contexts[current_context]);
        assert(rc >= 0);
    }

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
            if (debug) {
                fmt::print("remove fiber {}\n", old_context);
            }
        }
        by_interrupt = false;
        assert(!just_me->contexts.empty());
        if (prefix_size > 0) {
            just_me->current_context = (just_me->current_context + 1) % prefix_size;
            if (debug) {
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
        auto stack = reinterpret_cast<char*>(just_me->scheduler_stack);
        auto ret = VALGRIND_STACK_REGISTER(stack, stack + stack_size);
        memcheck_stacks.emplace_back(ret);
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
        auto stack_ptr =  reinterpret_cast<char*>(stack);
        auto ret = VALGRIND_STACK_REGISTER(stack_ptr, stack_ptr + stack_size);
        memcheck_stacks.emplace_back(ret);
        uc->uc_stack = {stack, 0, stack_size};
        uc->uc_link = &scheduler_context;
        rc = sigemptyset(&uc->uc_sigmask);
        assert(rc >= 0);
        auto ptr = reinterpret_cast<void (*)(void)>(fiber);
        makecontext(uc, ptr, 1, a);
        if (debug) {
            fmt::print("context: {}\n", static_cast<void*>(uc));
        }
    }

    static void setup_timer(long timer_us) noexcept {
        timeval tv = {0, timer_us};
        itimerval it = {tv, tv};
        auto rc = setitimer(ITIMER_REAL, &it, nullptr);
        assert(rc == 0);
    }

    constexpr static auto stack_size = 16'384u;
    static int timer_us;
    static cooperative_scheduler* just_me;
    sigset_t signal_mask_set;
    // used only in scheduler_interrupt, no need for volatile
    ucontext_t scheduler_context;
    // global interrupt stack
    void *scheduler_stack;
    std::vector<ucontext_t> contexts;
    std::vector<unsigned> memcheck_stacks;

    ucontext_t main_context;
    unsigned current_context = 0u;
    unsigned prefix_size;
    static volatile bool by_interrupt;
};

cooperative_scheduler* cooperative_scheduler::just_me = nullptr;
volatile bool cooperative_scheduler::by_interrupt = false;
bool cooperative_scheduler::debug = true;
int cooperative_scheduler::timer_us = (debug)? 33'000 : 10'000; // 33ms or 10ms
