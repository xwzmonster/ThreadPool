#include "common/threadpool_test_common.h"

/*
    - 慢关闭后不能再提交任务
    - 立即关闭时，尚未开始执行的排队任务会被丢弃
*/

TEST(ThreadPool, RejectPostAfterSlowShutdown) {
    threadpool_t* pool = threadpool_create(2);
    ASSERT_NE(pool, nullptr);

    threadpool_shutdown_slow(pool);

    EXPECT_NE(threadpool_post(pool, nop_task, nullptr), 0);

    threadpool_waitdone(pool);
}

TEST(ThreadPool, ImmediateShutdownDropsQueuedTasksThatHaveNotStarted) {
    BlockingState state;

    threadpool_t* pool = threadpool_create(2);
    ASSERT_NE(pool, nullptr);

    ASSERT_EQ(threadpool_post(pool, blocking_task, &state), 0);
    ASSERT_EQ(threadpool_post(pool, blocking_task, &state), 0);

    {
        std::unique_lock<std::mutex> lock(state.mu);
        state.cv_started.wait(lock, [&] {
            return state.started == 2;
        });
    }

    const int queued_n = 200;
    for (int i = 0; i < queued_n; ++i) {
        ASSERT_EQ(threadpool_post(pool, queued_task, &state), 0);
    }

    threadpool_shutdown_immediately(pool);

    {
        std::lock_guard<std::mutex> lock(state.mu);
        state.release = true;
    }
    state.cv_release.notify_all();

    threadpool_waitdone(pool);

    EXPECT_EQ(state.queued_done.load(std::memory_order_relaxed), 0);
}