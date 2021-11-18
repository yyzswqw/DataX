package com.alibaba.datax.monitor.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ThreadPoolService {

    private CountDownLatch countDownLatch;
    private ExecutorService accessExecutorService;
    private ExecutorService executorService;

    public ThreadPoolService(int concurrentNum,int maxPoolSize,int taskNum) {
        countDownLatch = new CountDownLatch(taskNum);
        this.accessExecutorService = new ThreadPoolExecutor(
                concurrentNum,150,20L, TimeUnit.MINUTES,new LinkedBlockingDeque<>(maxPoolSize),
                new ThreadFactory() {
                    AtomicLong i = new AtomicLong(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "thread-pool-access-" + i.getAndIncrement());
                        return t;
                    }
                },new ThreadPoolExecutor.CallerRunsPolicy()
        );
        int maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        this.executorService = new ThreadPoolExecutor(
                concurrentNum,
                concurrentNum > maximumPoolSize ? concurrentNum : maximumPoolSize,
                2L, TimeUnit.HOURS,
                new LinkedBlockingDeque<>(100),
                new ThreadFactory() {
                    AtomicLong i = new AtomicLong(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "thread-pool-worker-" + i.getAndIncrement());
                        return t;
                    }
                },
                (r, executor) -> {
                    BlockingQueue<Runnable> queue = executor.getQueue();
                    try {
//                    队列满了时，会一直阻塞等待着
                        queue.put(r);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void submit(Runnable r){
        this.accessExecutorService.submit(()->this.executorService.submit(r));
    }

    public <T> Future<T> call(Callable<T> c){
        Future<Future<T>> submit = this.accessExecutorService.submit(() -> this.executorService.submit(c));
        try {
            return submit.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void shutdown(){
        try {
            this.countDownLatch.await();
        } catch (InterruptedException e) {
            log.info("任务被打断");
            e.printStackTrace();
        }
        if (!this.executorService.isShutdown()) {
            this.executorService.shutdown();
        }
        if (!this.accessExecutorService.isShutdown()) {
            this.accessExecutorService.shutdown();
        }
    }


}
