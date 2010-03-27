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
package terrastore.communication.seda;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import terrastore.communication.protocol.Command;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class SEDAThreadPoolExecutorTest {

    @Test
    public void testExecution() throws Exception {
        Command command = createMock(Command.class);

        replay(command);

        int commands = 1000;
        final CountDownLatch waitThreadsExecution = new CountDownLatch(commands);

        SEDAThreadPoolExecutor pool = new SEDAThreadPoolExecutor();

        for (int i = 0; i < commands; i++) {
            pool.execute(command, new CommandHandler() {

                @Override
                public Object handle(Command command) throws Exception {
                    waitThreadsExecution.countDown();
                    return null;
                }
            });
        }

        assertTrue(waitThreadsExecution.await(60, TimeUnit.SECONDS));

        pool.shutdown();

        verify(command);
    }

    @Test
    public void testPauseWaitsActiveThreads() throws Exception {
        Command command = createMock(Command.class);

        replay(command);

        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        SEDAThreadPoolExecutor pool = new SEDAThreadPoolExecutor();

        for (int i = 0; i < commands; i++) {
            pool.execute(command, new CommandHandler() {

                @Override
                public Object handle(Command command) throws Exception {
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

        verify(command);
    }

    @Test
    public void testPauseIgnoresExecutionRequests() throws Exception {
        Command command = createMock(Command.class);

        replay(command);

        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        SEDAThreadPoolExecutor pool = new SEDAThreadPoolExecutor();

        pool.pause();

        for (int i = 0; i < commands; i++) {
            pool.execute(command, new CommandHandler() {

                @Override
                public Object handle(Command command) throws Exception {
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

        verify(command);
    }

    @Test
    public void testPauseAndResumeExecutionRequests() throws Exception {
        Command command = createMock(Command.class);

        replay(command);

        int commands = 1000;
        final CountDownLatch waitThreadsStart = new CountDownLatch(commands);
        final AtomicInteger count = new AtomicInteger(commands);

        SEDAThreadPoolExecutor pool = new SEDAThreadPoolExecutor();

        pool.pause();

        for (int i = 0; i < commands; i++) {
            pool.execute(command, new CommandHandler() {

                @Override
                public Object handle(Command command) throws Exception {
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

        verify(command);
    }
}
