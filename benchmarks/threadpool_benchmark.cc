#include <iostream>
#include <atomic>
#include <chrono>

extern "C" {
#include "../thread_pool.h"
}

static std::atomic<int> g_done{0};

static void task(void* arg) {
    (void)arg;
    g_done.fetch_add(1, std::memory_order_relaxed);
}

int main() {
    const int N = 1000000;

    threadpool_t* pool = threadpool_create(4);
    if (!pool) {
        std::cerr << "threadpool_create failed\n";
        return 1;
    }

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < N; ++i) {
        if (threadpool_post(pool, task, nullptr) != 0) {
            std::cerr << "threadpool_post failed at " << i << "\n";
            threadpool_shutdown_slow(pool);
            threadpool_waitdone(pool);
            return 2;
        }
    }

    threadpool_shutdown_slow(pool);
    threadpool_waitdone(pool);

    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "Tasks: " << N << "\n";
    std::cout << "Time: " << ms << " ms\n";
    if (ms > 0) {
        std::cout << "Throughput: " << (N * 1000.0 / ms) << " tasks/sec\n";
    }

    return 0;
}