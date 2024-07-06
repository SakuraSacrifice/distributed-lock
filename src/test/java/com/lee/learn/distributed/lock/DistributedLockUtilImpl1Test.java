package com.lee.learn.distributed.lock;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public class DistributedLockUtilImpl1Test {

    @Test
    public void 简单测试多线程下加锁效果() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(() -> {
            DistributedLockUtilImpl1.LockKey lockKey = DistributedLockUtilImpl1.LockKey.COMMON_KEY;

            if (DistributedLockUtilImpl1.tryLock(lockKey)) {
                try {
                    System.out.println(Thread.currentThread().getName() + " -> hold the lock for 10 seconds");
                    LockSupport.parkNanos(1000 * 1000 * 1000 * 10L);
                    System.out.println(Thread.currentThread().getName() + " -> now release the lock");
                } finally {
                    boolean unlockSuccess = DistributedLockUtilImpl1.unLock(lockKey);
                    if (!unlockSuccess) {
                        System.out.println(Thread.currentThread().getName() + " -> release lock failed");
                    }
                }
            } else {
                System.out.println(Thread.currentThread().getName() + " -> failed to hold the lock");
            }

            countDownLatch.countDown();
        }, "thread-1").start();

        LockSupport.parkNanos(1000 * 1000 * 100);

        new Thread(() -> {
            DistributedLockUtilImpl1.LockKey lockKey = DistributedLockUtilImpl1.LockKey.COMMON_KEY;

            DistributedLockUtilImpl1.lock(lockKey);
            try {
                System.out.println(Thread.currentThread().getName() + " -> hold the lock for 10 seconds");
                LockSupport.parkNanos(1000 * 1000 * 1000 * 10L);
                System.out.println(Thread.currentThread().getName() + " -> now release the lock");
            } finally {
                boolean unLockSuccess = DistributedLockUtilImpl1.unLock(lockKey);
                if (!unLockSuccess) {
                    System.out.println(Thread.currentThread().getName() + " -> release lock failed");
                }
            }
            countDownLatch.countDown();
        }, "thread-2").start();

        countDownLatch.await();
    }

}