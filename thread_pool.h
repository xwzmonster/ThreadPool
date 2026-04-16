#ifndef THREAD_POOL_H
#define THREAD_POOL_H

typedef struct threadpool_s threadpool_t;
typedef void (*handler_pt)(void*); // (void*): 这里是上下文, 通过参数传递进去

#ifdef __cplusplus 
extern "C"
{
#endif

threadpool_t* threadpool_create(int thread_count); // 创建并启动一个包含 thread_count 个 worker 的线程池对象

/* 
    可多生产者并发调用; 
    仅在线程池处于运行状态时成功; 关闭后返回失败
    在非 TP_RUNNING 状态下调用threadpool_post(), 会失败返回。
    任务参数arg 的生命周期这里设置为调用者管理，线程池只管理任务节点本身，不释放arg指向的内存
*/
int threadpool_post(threadpool_t* pool, handler_pt func, void* arg); 

/* 
    等待 worker 退出并释放线程池对象, 调用 threadpool_waitdone() 前, 调用者必须保证不会再有任何线程使用这个pool
    假设  
        1. 线程 A 是 producer，正在循环 threadpool_post(pool, ...)
        2. 线程 B 是主线程，调用了 threadpool_shutdown(pool)
        3. 然后线程 B 紧接着调用 threadpool_waitdone(pool)
        4. worker 都退出了
        5. waitdone() 把 queue 和 pool 都释放掉了
        6. 这时线程 A 下一轮又执行 threadpool_post(pool, ...)
    结果可能: 
        - 线程 A 手里那个 pool 指针，虽然“值还在”，但它指向的内存已经被释放了
        - 它再读 pool->task_queue，就是读野指针
        - 它再锁 queue->mutex，就是锁一块已经被释放的内存
        - 这就是 use-after-free，可能崩，也可能更隐蔽地错
    
    对象生命周期没有被调用方和库一起管好
    
*/
// 如果线程池仍在运行，则自动触发缓慢关闭；等待所有 worker 退出并释放线程池对象；调用前必须保证不会再有其他线程访问该 pool
void threadpool_waitdone(threadpool_t* pool);

void threadpool_shutdown_slow(threadpool_t* pool); // 停止接收新任务, 但执行完队列中的任务

void threadpool_shutdown_immediately(threadpool_t* pool); // 停止接收新任务, worker 不再取新任务; 已经在执行的任务继续执行; 未开始的排队任务会被丢弃

#ifdef __cplusplus
}
#endif

#endif