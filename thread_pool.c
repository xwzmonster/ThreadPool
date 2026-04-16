#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include "thread_pool.h"

typedef struct task_s {
    struct task_s* next; // 下一个任务结点
    handler_pt func; // 任务函数
    void *arg; // 任务执行函数 func 的参数
} task_t; // 一个任务结点 

typedef struct task_queue_s {
    task_t* head;
    task_t** tail; 
    /*
        让“检查条件”和“进入等待”这两件事, 对别的线程来说看起来是连在一起的
    */
    pthread_mutex_t mutex;  // 互斥锁, 保护“线程是否该等待”的状态, 并让 pthread_cond_wait() 正常工作
    pthread_cond_t cond; // 条件变量, 和mutex一起用来实现“队列空时休眠, 来任务时唤醒”
} task_queue_t;

typedef enum {
    TP_RUNNING = 0,
    TP_SHUTDOWN_SLOW,
    TP_SHUTDOWN_IMMEDIATE
} threadpool_state_t;

struct threadpool_s {
    task_queue_t *task_queue;
    threadpool_state_t state;
    int thrd_count;
    pthread_t *threads; 
};

// 创建对象的时候, 回滚式的
static task_queue_t* __taskqueue_create() { // 初始化任务队列
    task_queue_t* queue = (task_queue_t*)malloc(sizeof(*queue));
    if(!queue) {
        return NULL;
    }
    int ret = pthread_mutex_init(&queue->mutex, NULL);
    if(ret != 0) {
        free(queue);
        return NULL;
    }

    ret = pthread_cond_init(&queue->cond, NULL);
    if(ret != 0) {
        pthread_mutex_destroy(&queue->mutex);
        free(queue);
        return NULL;
    }
    queue->head = NULL;
    queue->tail = &queue->head;
    return queue;
}

// 生产者线程把任务安全地挂到队尾, 然后叫醒一个可能正在等活干的 worker线程
static inline void __add_task_locked(task_queue_t *queue, task_t *task) { 
    task->next = NULL;
    *queue->tail = task;
    queue->tail = &task->next;
    pthread_cond_signal(&queue->cond);
}

// 如果队列非空, 立刻弹出一个任务; 如果为空, 直接返回 NULL
static inline task_t* __pop_task_locked(task_queue_t *queue) {
    if (queue->head == NULL) {
        return NULL;
    }
    task_t *task;
    task = queue->head;
    queue->head = task->next;
    // task->next = NULL; 
    if (queue->head == NULL) {
        queue->tail = &queue->head;
    }
    return task;
}

static inline task_t* __get_task(threadpool_t* pool) {  // 获取任务
    task_queue_t* queue = pool->task_queue;
    pthread_mutex_lock(&queue->mutex);

    while(queue->head == NULL && pool->state == TP_RUNNING) {
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }
    
    // 立即停机：不再取任务, 直接退出
    if(pool->state == TP_SHUTDOWN_IMMEDIATE) {
        pthread_mutex_unlock(&queue->mutex);
        return NULL;
    }

    // 缓慢停止, 不再接新任务, 且要等到剩下任务都执行完, 任务队列空, 然后退出
    if(queue->head == NULL && pool->state == TP_SHUTDOWN_SLOW) {
        pthread_mutex_unlock(&queue->mutex);
        return NULL;
    }

    task_t* task = __pop_task_locked(queue);
    pthread_mutex_unlock(&queue->mutex);
    return task;
}


/* 
    因为arg 指向的数据的生命周期可能不明确, 不一定由任务队列统一拥有和统一释放的.
    所以任务函数参数即arg指针实际指向的那块内存没有随着任务释放而释放
*/
static void __taskqueue_destroy(task_queue_t *queue) {
    pthread_mutex_lock(&queue->mutex);
    task_t *task;
    while ((task = __pop_task_locked(queue))) {
        free(task);
    }
    pthread_mutex_unlock(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
    pthread_mutex_destroy(&queue->mutex);
    free(queue);
}

static void* __threadpool_worker(void *arg) {  // 工作线程入口
    threadpool_t *pool = (threadpool_t*) arg;
    task_t *task;
    void *ctx;

    for(;;) {
        task = (task_t*)__get_task(pool);
        if (!task) {
            break;
        }
        handler_pt func = task->func;
        ctx = task->arg;
        free(task);
        func(ctx);
    }
    
    return NULL;
}

// 创建线程失败时的回滚退出
static void __threads_terminate(threadpool_t* pool) {
    task_queue_t* queue = pool->task_queue;
    pthread_mutex_lock(&queue->mutex);
    pool->state = TP_SHUTDOWN_IMMEDIATE;
    pthread_cond_broadcast(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);

    for(int i = 0; i < pool->thrd_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    
}

static int __threads_create(threadpool_t *pool, int thrd_count) {
    if(thrd_count <= 0) {
        return -1;
    }
    pthread_attr_t attr; 
	int ret;
    ret = pthread_attr_init(&attr);
    if (ret == 0) {
        pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thrd_count);
        if (pool->threads) {
            int i = 0;
            for (; i < thrd_count; i++) {
                if (pthread_create(&pool->threads[i], &attr, __threadpool_worker, pool) != 0) {
                    break; // 线程池创建要么创建完整个线程池, 要么认为失败并回滚. 即不会接受“本来要8个线程, 结果只建成3个也算成功”
                }
            }
            pool->thrd_count = i; // 保证后续清理只处理已经成功创建出来的线程
            pthread_attr_destroy(&attr);
            if (i == thrd_count)
                return 0;
            __threads_terminate(pool);
            free(pool->threads);
            pool->threads = NULL;
            pool->thrd_count = 0;
        }
        ret = -1;
    }
    return ret;
}

// 线程池慢慢关闭, 停止接收新任务, 但把队列里的任务做完
void threadpool_shutdown_slow(threadpool_t * pool) {
    task_queue_t* queue = pool->task_queue;

    pthread_mutex_lock(&queue->mutex);
    if(pool->state == TP_RUNNING) {
        pool->state = TP_SHUTDOWN_SLOW;
        pthread_cond_broadcast(&queue->cond);
    } 
    pthread_mutex_unlock(&queue->mutex);
}

// 立即关闭---停止接收新任务, worker 不再取新任务；已经在执行的任务继续跑完, 队列里剩下的任务丢弃
void threadpool_shutdown_immediately(threadpool_t* pool) {
    task_queue_t* queue = pool->task_queue;

    pthread_mutex_lock(&queue->mutex);
    if(pool->state != TP_SHUTDOWN_IMMEDIATE) {
        pool->state = TP_SHUTDOWN_IMMEDIATE;
        pthread_cond_broadcast(&queue->cond);
    }
    pthread_mutex_unlock(&queue->mutex);
}

threadpool_t* threadpool_create(int thrd_count) {
    if(thrd_count <= 0) {
        return NULL;
    }
    threadpool_t *pool;
    pool = (threadpool_t*) malloc(sizeof(*pool));
    if (!pool) {
        return NULL;
    }
    task_queue_t *queue = __taskqueue_create();
    if (queue) {
        pool->task_queue = queue;
        pool->state = TP_RUNNING;
        if (__threads_create(pool, thrd_count) == 0) {
            return pool;
        }
        __taskqueue_destroy(pool->task_queue);
    }
    free(pool);
    return NULL;
}

int threadpool_post(threadpool_t *pool, handler_pt func, void *arg) {
    if(!pool || !func) {
        return -1;
    }
    task_t *task = (task_t *)malloc(sizeof(task_t));
    if (!task) {
        return -1;
    }
    task->func = func;
    task->arg = arg;
    task->next = NULL;
    task_queue_t* queue = pool->task_queue;
    pthread_mutex_lock(&queue->mutex);
    if(pool->state != TP_RUNNING) {
        pthread_mutex_unlock(&queue->mutex);
        free(task);
        return -1;
    }
    __add_task_locked(queue, task);
    pthread_mutex_unlock(&queue->mutex);
    return 0;
}

// 如果线程池还在运行, 就自动触发一次缓慢关闭; 然后等待 worker线程退出并释放对象
void threadpool_waitdone(threadpool_t *pool) {
    if(!pool) {
        return ;
    }
    task_queue_t* queue = pool->task_queue;
    pthread_mutex_lock(&queue->mutex);
    if(pool->state == TP_RUNNING) {
        pool->state = TP_SHUTDOWN_SLOW;
        pthread_cond_broadcast(&queue->cond);
    }
    pthread_mutex_unlock(&queue->mutex);
    for (int i = 0; i < pool->thrd_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    __taskqueue_destroy(pool->task_queue);
    free(pool->threads);
    free(pool);
}