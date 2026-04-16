# C Thread Pool

一个使用 `pthread + mutex + condition_variable` 实现的 C 语言线程池项目。

项目支持：

- 多生产者并发提交任务
- worker 线程阻塞等待任务
- 优雅关闭（slow shutdown）
- 立即关闭（immediate shutdown）
- 基于 GoogleTest 的正确性测试
- 简单吞吐 benchmark

## 1. 项目简介

这个项目实现了一个基于 POSIX 线程库的线程池。

线程池内部使用：

- 单链表任务队列
- `pthread_mutex_t` 保护共享状态
- `pthread_cond_t` 实现“队列空时休眠、来任务时唤醒”
- 三态状态机管理线程池生命周期

适合作为并发编程、线程同步、线程池基础设计的练习项目。

## 2. 功能特性

当前实现支持以下能力：

- `threadpool_create(int thread_count)`
    创建并启动指定数量的 worker 线程

- `threadpool_post(threadpool_t* pool, handler_pt func, void* arg)`
    多生产者并发安全地提交任务

- `threadpool_shutdown_slow(threadpool_t* pool)`
    停止接收新任务, 但执行完队列中已有任务

- `threadpool_shutdown_immediately(threadpool_t* pool)`
    停止接收新任务, worker 不再取新任务；已在执行中的任务继续执行；尚未开始执行的排队任务会被丢弃

- `threadpool_waitdone(threadpool_t* pool)`
    如果线程池仍在运行, 则自动触发一次 slow shutdown, 然后等待所有 worker 退出并释放线程池对象

## 3. 核心设计

### 3.1 任务队列

任务队列使用单链表实现：

- `head` 指向队首任务
- `tail` 不是尾节点本身, 而是“尾节点 next 指针的地址”

这种写法的好处是：

- 尾插是 O(1)
- 空队列和非空队列可以统一处理
- 不需要单独区分“是否是第一个节点”

### 3.2 同步机制

线程池使用一把 `mutex` 和一个 `condition variable` 管理任务队列和状态：

- producer 提交任务时：
- 持锁检查状态
- 尾插任务
- `signal` 唤醒一个可能正在等待的 worker

- worker 取任务时：
- 持锁检查“队列是否为空 + 当前状态”
- 如果队列空且线程池还在运行, 则进入 `pthread_cond_wait`
- 被唤醒后重新检查条件
- 满足取任务条件时弹出任务并执行

这里必须使用 `while` 而不是 `if`, 原因是：

- 条件变量可能发生虚假唤醒
- 被唤醒后条件未必仍然成立
- `while` 可以保证每次唤醒后重新检查条件

## 4. 状态机设计

线程池内部使用三态状态机：

- `TP_RUNNING`
    正常运行, 可以提交任务, worker 可以继续取任务

- `TP_SHUTDOWN_SLOW`
    停止接收新任务, 但 worker 会继续把队列中已有任务执行完

- `TP_SHUTDOWN_IMMEDIATE`
    停止接收新任务, worker 不再取新任务；队列中尚未开始的任务最终会被丢弃

相比早期使用两个标志位拼状态的做法, 单一状态机的优点是：

- 语义更清晰
- 不存在无效状态组合
- 代码更容易读懂和维护
- 更容易在测试和文档中描述

## 5. 生命周期约束

这个项目当前采用的是“调用方负责对象生命周期协议”的设计。

重要约束：

- `threadpool_waitdone()` 会释放线程池对象本身
- 调用 `threadpool_waitdone()` 前, 调用者必须保证不会再有任何线程访问该 `pool`
- 否则可能出现 use-after-free

例如：

- producer 线程仍在并发调用 `threadpool_post(pool, ...)`
- 主线程调用 `threadpool_waitdone(pool)` 并释放对象
- producer 后续再次访问 `pool`
- 就可能读取已经释放的内存

因此, 正确使用顺序应该是：

1. 通知所有 producer 停止提交任务
2. `join` 所有 producer 线程
3. 调用 `threadpool_shutdown_slow()` 或 `threadpool_shutdown_immediately()`
4. 调用 `threadpool_waitdone()`

另外, 任务参数 `arg` 的生命周期由调用者负责, 线程池只管理任务节点本身, 不释放 `arg` 指向的内存。

## 6. 测试

项目使用 GoogleTest 编写了以下测试：

- `WaitdoneWithoutExplicitShutdown`
验证不显式 shutdown 时, `waitdone()` 会自动触发 slow shutdown 并正常退出

- `RejectNullPoolOrNullFunc`
验证非法参数会被拒绝

- `BasicExactOnceAndSlowShutdown`
验证 slow shutdown 下所有任务都被恰好执行一次

- `RejectPostAfterSlowShutdown`
验证 slow shutdown 后不能再提交新任务

- `ImmediateShutdownDropsQueuedTasksThatHaveNotStarted`
验证 immediate shutdown 会丢弃尚未开始执行的排队任务

- `MultiProducerExactOnce`
验证多生产者并发提交场景下, 所有任务仍然恰好执行一次

## 7. Benchmark

项目提供了一个简单 benchmark：

- 创建固定数量 worker
- 提交大量轻量任务
- 统计总耗时和吞吐量

示例输出：

```text
Tasks: 100000
Time: 143 ms
Throughput: 699301 tasks/sec
```

注意：

- benchmark 只能反映大致吞吐
- benchmark 不能替代正确性测试

## 8. 编译与运行

### 编译线程池对象文件

gcc -std=c11 -Wall -Wextra -O2 -pthread -c thread_pool.c -o thread_pool.o

### 编译测试

g++ -std=c++17 -Wall -Wextra -O2 -pthread \
    tests/threadpool_basic_test.cc \
    tests/threadpool_shutdown_test.cc \
    tests/threadpool_stress_test.cc \
    thread_pool.o \
    -lgtest -lgtest_main \
    -o threadpool_test

### 运行测试

./threadpool_test

### 编译 benchmark

g++ -std=c++17 -Wall -Wextra -O2 -pthread \
    benchmarks/threadpool_benchmark.cc \
    thread_pool.o \
    -o threadpool_benchmark

### 运行 benchmark

./threadpool_benchmark

## 9. 已知限制

当前版本仍有以下限制：

- threadpool_waitdone() 前必须确保没有其他线程继续访问 pool
- 没有实现引用计数或更复杂的对象生命周期管理
- 没有实现任务优先级、动态扩缩容、future/promise 等高级特性

这些限制是当前项目设计范围的一部分, 而不是遗漏的 bug。

## 10. 后续改进

可以继续扩展的方向包括：

- 更严格的对象所有权模型
- 引用计数或更完整的关闭协议
- 动态扩缩容
- 任务优先级队列
- future / promise 风格结果返回
- 更系统的 benchmark 和性能分析
