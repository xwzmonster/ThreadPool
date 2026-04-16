#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

extern "C" {
#include "../../thread_pool.h"
}

struct ExactOnceState {
    std::vector<int> seen;
    std::mutex seen_mu;
    std::atomic<int> done{0};

    explicit ExactOnceState(int n) : seen(n, 0) {}
};

struct ExactOnceArg {
    ExactOnceState* state;
    int id;
};

static void exact_once_task(void* arg) {
    auto* a = static_cast<ExactOnceArg*>(arg);

    {
        std::lock_guard<std::mutex> lock(a->state->seen_mu);
        a->state->seen[a->id] += 1;
    }

    a->state->done.fetch_add(1, std::memory_order_relaxed);
}

static void nop_task(void* arg) {
    (void)arg;
}

// --------------------
// 2. 立即关闭测试用辅助结构
//    先用两个阻塞任务占满 worker，
//    再投排队任务，然后立即关闭，
//    这样排队任务应该被丢弃
// --------------------

struct BlockingState {
    std::mutex mu;
    std::condition_variable cv_started;
    std::condition_variable cv_release;
    int started = 0;
    bool release = false;
    std::atomic<int> queued_done{0};
};

static void blocking_task(void* arg) {
    auto* state = static_cast<BlockingState*>(arg);

    std::unique_lock<std::mutex> lock(state->mu);
    state->started += 1;
    state->cv_started.notify_one();

    state->cv_release.wait(lock, [&] {
        return state->release;
    });
}

static void queued_task(void* arg) {
    auto* state = static_cast<BlockingState*>(arg);
    state->queued_done.fetch_add(1, std::memory_order_relaxed);
}