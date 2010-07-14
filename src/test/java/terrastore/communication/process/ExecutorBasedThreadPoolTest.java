/**
 * Copyright 2009 - 2010 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.communication.process;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ExecutorBasedThreadPoolTest {

    @Test
    public void testExecution() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsExecution = new CountDownLatch(commands);

        ExecutorBasedThreadPool pool = new ExecutorBasedThreadPool();

        for (int i = 0; i < commands; i++) {
            pool.execute(new Callable() {

                @Override
                public Object call() throws Exception {
                    waitThreadsExecution.countDown();
                    return null;
                }
            });
        }

        assertTrue(waitThreadsExecution.await(60, TimeUnit.SECONDS));

        pool.shutdown();
    }

    @Test
    public void testPauseWaitsActiveThreads() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        ExecutorBasedThreadPool pool = new ExecutorBasedThreadPool();

        for (int i = 0; i < commands; i++) {
            pool.execute(new Callable() {

                @Override
                public Object call() throws Exception {
                    try {
                        waitThreadsStart.countDown();
                        Thread.sleep(10);
                        count.decrementAndGet();
                        return null;
                    } catch (InterruptedException ex) {
                        return null;
                    }
                }
            });
        }

        waitThreadsStart.await();
        pool.pause();

        assertEquals(0, count.get());

        pool.shutdown();
    }

    @Test
    public void testPauseIgnoresExecutionRequests() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        ExecutorBasedThreadPool pool = new ExecutorBasedThreadPool();

        pool.pause();

        for (int i = 0; i < commands; i++) {
            pool.execute(new Callable() {

                @Override
                public Object call() throws Exception {
                    try {
                        waitThreadsStart.countDown();
                        Thread.sleep(10);
                        count.decrementAndGet();
                        return null;
                    } catch (InterruptedException ex) {
                        return null;
                    }
                }
            });
        }

        assertFalse(waitThreadsStart.await(1, TimeUnit.SECONDS));
        assertEquals(commands, count.get());

        pool.shutdown();
    }

    @Test
    public void testPauseAndResumeExecutionRequests() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        ExecutorBasedThreadPool pool = new ExecutorBasedThreadPool();

        pool.pause();

        for (int i = 0; i < commands; i++) {
            pool.execute(new Callable() {

                @Override
                public Object call() throws Exception {
                    try {
                        waitThreadsStart.countDown();
                        Thread.sleep(10);
                        count.decrementAndGet();
                        return null;
                    } catch (InterruptedException ex) {
                        return null;
                    }
                }
            });
        }

        assertFalse(waitThreadsStart.await(1, TimeUnit.SECONDS));
        assertEquals(commands, count.get());

        pool.resume();

        assertTrue(waitThreadsStart.await(60, TimeUnit.SECONDS));

        pool.shutdown();
    }
}
