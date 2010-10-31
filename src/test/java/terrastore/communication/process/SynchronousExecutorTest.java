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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SynchronousExecutorTest {

    @Test
    public void testSynchronousExecution() throws Exception {
        SynchronousExecutor executor = new SynchronousExecutor();
        Future future = executor.execute(new Callable() {

            @Override
            public Object call() throws Exception {
                return "Done";
            }
        });

        assertEquals("Done", future.get());
        assertTrue(future.isDone());

        executor.shutdown();
    }

    @Test
    public void testConcurrentExecution() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsExecution = new CountDownLatch(commands);

        final SynchronousExecutor executor = new SynchronousExecutor();
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < commands; i++) {
            pool.execute(new Runnable() {

                @Override
                public void run() {
                    executor.execute(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            waitThreadsExecution.countDown();
                            return null;
                        }
                    });
                }
            });
        }

        assertTrue(waitThreadsExecution.await(60, TimeUnit.SECONDS));

        executor.shutdown();
        pool.shutdown();
    }

    @Test
    public void testPauseWaitsAllExecutionsToFinish() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsExecution = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        final SynchronousExecutor executor = new SynchronousExecutor();
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < commands; i++) {
            pool.execute(new Runnable() {

                @Override
                public void run() {
                    executor.execute(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            try {
                                waitThreadsExecution.countDown();
                                count.decrementAndGet();
                                return null;
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            });
        }

        assertTrue(waitThreadsExecution.await(60, TimeUnit.SECONDS));

        executor.pause();

        assertEquals(0, count.get());

        executor.shutdown();
        pool.shutdown();
    }

    @Test
    public void testPauseHoldsSubsequentExecutionRequests() throws Exception {
        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        final SynchronousExecutor executor = new SynchronousExecutor();

        executor.pause();

        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < commands; i++) {
            pool.execute(new Runnable() {

                @Override
                public void run() {
                    executor.execute(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            try {
                                waitThreadsStart.countDown();
                                count.decrementAndGet();
                                return null;
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            });
        }

        assertFalse(waitThreadsStart.await(1, TimeUnit.SECONDS));
        assertEquals(commands, count.get());

        executor.shutdown();
        pool.shutdown();
    }

    @Test
    public void testResumeExecutesPreviouslyHeldRequests() throws Exception {
        int commands = 1000;
        final CountDownLatch executionCount = new CountDownLatch(commands);

        final SynchronousExecutor executor = new SynchronousExecutor();

        executor.pause();

        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < commands; i++) {
            pool.execute(new Runnable() {

                @Override
                public void run() {
                    executor.execute(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            try {
                                executionCount.countDown();
                                return null;
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            });
        }

        executor.resume();

        assertTrue(executionCount.await(60, TimeUnit.SECONDS));

        executor.shutdown();
        pool.shutdown();
    }

    @Test
    public void testConcurrentPauseResume() throws Exception {
        int commands = 1000;
        final CountDownLatch executionCount = new CountDownLatch(commands);

        final SynchronousExecutor executor = new SynchronousExecutor();
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < commands; i++) {
            if (i == 100) {
                Thread t = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        executor.pause();
                    }
                });
                t.start();
                t.join();
            } else if (i == 500) {
                Thread t = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        executor.resume();
                    }
                });
                t.start();
                t.join();
            }
            pool.execute(new Runnable() {

                @Override
                public void run() {
                    executor.execute(new Callable() {

                        @Override
                        public Object call() throws Exception {
                            try {
                                executionCount.countDown();
                                return null;
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            });
        }

        assertTrue(executionCount.await(60, TimeUnit.SECONDS));

        executor.shutdown();
        pool.shutdown();
    }
}
