#include "common/threadpool_test_common.h"

/*
    - 多生产者并发提交时
    - 所有任务都恰好执行一次
    - 没有提交失败
*/

TEST(ThreadPool, MultiProducerExactOnce) {
    const int kWorkers = 4;
    const int kProducers = 4;
    const int kTasksPerProducer = 25000;
    const int N = kProducers * kTasksPerProducer;

    ExactOnceState state(N);
    std::vector<ExactOnceArg> args;
    args.reserve(N);

    for (int i = 0; i < N; ++i) {
        args.push_back(ExactOnceArg{&state, i});
    }

    threadpool_t* pool = threadpool_create(kWorkers);
    ASSERT_NE(pool, nullptr);

    std::atomic<int> failed_posts{0};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);

    for (int p = 0; p < kProducers; ++p) {
        const int begin = p * kTasksPerProducer;
        const int end = begin + kTasksPerProducer;

        producers.emplace_back([pool, &args, begin, end, &failed_posts] {
            for (int i = begin; i < end; ++i) {
                if (threadpool_post(pool, exact_once_task, &args[i]) != 0) {
                    failed_posts.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    EXPECT_EQ(failed_posts.load(std::memory_order_relaxed), 0);

    threadpool_shutdown_slow(pool);
    threadpool_waitdone(pool);

    EXPECT_EQ(state.done.load(std::memory_order_relaxed), N);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(state.seen[i], 1) << "task id = " << i;
    }
}