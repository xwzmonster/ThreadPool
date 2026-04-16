#include "common/threadpool_test_common.h"
/*
    - 不显式 shutdown，直接 waitdone() 是否正常
    - 空参数是否被拒绝
    - 优雅关闭下任务是否恰好执行一次
*/

TEST(ThreadPool, WaitdoneWithoutExplicitShutdown) {
    threadpool_t* pool = threadpool_create(2);
    ASSERT_NE(pool, nullptr);

    threadpool_waitdone(pool);

    SUCCEED();
}

TEST(ThreadPool, RejectNullPoolOrNullFunc) {
    EXPECT_NE(threadpool_post(nullptr, nop_task, nullptr), 0);

    threadpool_t* pool = threadpool_create(2);
    ASSERT_NE(pool, nullptr);

    EXPECT_NE(threadpool_post(pool, nullptr, nullptr), 0);

    threadpool_waitdone(pool);
}

TEST(ThreadPool, BasicExactOnceAndSlowShutdown) {
    const int N = 10000;

    ExactOnceState state(N);
    std::vector<ExactOnceArg> args;
    args.reserve(N);

    for (int i = 0; i < N; ++i) {
        args.push_back(ExactOnceArg{&state, i});
    }

    threadpool_t* pool = threadpool_create(4);
    ASSERT_NE(pool, nullptr);

    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(threadpool_post(pool, exact_once_task, &args[i]), 0);
    }

    threadpool_shutdown_slow(pool);
    threadpool_waitdone(pool);

    EXPECT_EQ(state.done.load(std::memory_order_relaxed), N);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(state.seen[i], 1) << "task id = " << i;
    }
}