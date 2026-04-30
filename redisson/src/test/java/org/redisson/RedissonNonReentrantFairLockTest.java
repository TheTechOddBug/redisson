package org.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RedissonNonReentrantFairLockTest extends RedisDockerTest {

    @Test
    public void testContendingThreadBlocksAndAcquiresOnRelease() throws InterruptedException {
        RLock lock = redisson.getNonReentrantFairLock("nrfl:contend");
        lock.lock();

        AtomicBoolean acquired = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            started.countDown();
            lock.lock();
            try {
                acquired.set(true);
            } finally {
                lock.unlock();
            }
        });
        t.start();

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        assertThat(acquired).isFalse();

        lock.unlock();
        t.join(5_000);
        assertThat(t.isAlive()).isFalse();
        assertThat(acquired).isTrue();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    public void testSameThreadReacquireThrows() {
        RLock lock = redisson.getNonReentrantFairLock("nrfl:self");
        lock.lock();
        try {
            assertThatThrownBy(lock::lock).isInstanceOf(IllegalMonitorStateException.class);
            assertThatThrownBy(lock::tryLock).isInstanceOf(IllegalMonitorStateException.class);
            assertThatThrownBy(() -> lock.tryLock(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(IllegalMonitorStateException.class);

            // Counter never bumped: still held by us, single unlock fully releases.
            assertThat(lock.isHeldByCurrentThread()).isTrue();
        } finally {
            lock.unlock();
        }
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    public void testCrossInstanceSameThreadReentryThrows() {
        RLock a = redisson.getNonReentrantFairLock("nrfl:cross");
        RLock b = redisson.getNonReentrantFairLock("nrfl:cross");
        assertThat(a).isNotSameAs(b);

        a.lock();
        try {
            assertThatThrownBy(b::lock).isInstanceOf(IllegalMonitorStateException.class);
            assertThatThrownBy(b::tryLock).isInstanceOf(IllegalMonitorStateException.class);
            assertThat(b.isHeldByCurrentThread()).isTrue();
        } finally {
            a.unlock();
        }
        assertThat(a.isLocked()).isFalse();
        assertThat(b.isLocked()).isFalse();
    }
}
