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
package terrastore.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class GlobalExecutorTest {

    @Test
    public void testTaskCancellationOnTimeout() throws Exception {
        final CountDownLatch exitLatch = new CountDownLatch(1);
        Future future = null;
        try {
            long timeout = 1000;
            Runnable task = new Runnable() {

                @Override
                public void run() {
                    try {
                        while (true) {
                            ConcurrentUtils.exitOnTimeout();
                        }
                    } catch (RuntimeException ex) {
                        exitLatch.countDown();
                        throw ex;
                    }
                }

            };
            //
            future = GlobalExecutor.getQueryExecutor().submit(task);
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            assertTrue(future.cancel(true));
            exitLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
