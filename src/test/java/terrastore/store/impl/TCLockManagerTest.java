/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.store.impl;

import terrastore.internal.tc.TCMaster;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import terrastore.store.Key;
import terrastore.store.LockManager;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCLockManagerTest {

    @Before
    public void setUp() {
        TCMaster.getInstance().setupLocally();
    }

    @Test
    public void testConcurrencyLevel() throws InterruptedException {
        final int concurrencyLevel = 2;
        final int threads = 100;
        final int repeat = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final LockManager lockManager = new TCLockManager("node", concurrencyLevel);
        final CountDownLatch concurrentLatch = new CountDownLatch(concurrencyLevel);
        for (int i = 0; i < repeat; i++) {
            final String key = "" + i;
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    lockManager.lockWrite("bucket", new Key(key));
                    try {
                        concurrentLatch.countDown();
                        concurrentLatch.await(60, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        lockManager.unlockWrite("bucket", new Key(key));
                    }
                }

            });
        }
        assertTrue(concurrentLatch.await(60, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrencyLevelIsNotOverflow() throws InterruptedException {
        final AtomicInteger concurrentThreads = new AtomicInteger(0);
        final AtomicBoolean overflow = new AtomicBoolean(false);
        final int concurrencyLevel = 10;
        final int threads = 100;
        final int repeat = 1000;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final LockManager lockManager = new TCLockManager("node", concurrencyLevel);
        for (int i = 0; i < repeat; i++) {
            final String key = "" + i;
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    lockManager.lockWrite("bucket", new Key(key));
                    try {
                        if (concurrentThreads.incrementAndGet() > concurrencyLevel) {
                            overflow.set(true);
                        } else {
                            concurrentThreads.decrementAndGet();
                        }
                    } finally {
                        lockManager.unlockWrite("bucket", new Key(key));
                    }
                }

            });
        }
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        assertFalse(overflow.get());
    }

}
