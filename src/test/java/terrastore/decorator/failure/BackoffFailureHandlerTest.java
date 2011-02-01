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
package terrastore.decorator.failure;

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class BackoffFailureHandlerTest {

    @Test()
    public void testSuccessfulBackoff() throws Exception {
        Delegate delegate = createMock(Delegate.class);
        delegate.test();
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(1, "Expected error."))).times(1).andReturn("test").times(1);

        replay(delegate);

        try {
            Delegate proxy = BackoffFailureHandler.newInstance(delegate, Delegate.class, 1, 1000);
            assertEquals("test", proxy.test());
        } finally {
            verify(delegate);
        }
    }

    @Test(expected = CommunicationException.class)
    public void testBackoffWithException() throws Exception {
        Delegate delegate = createMock(Delegate.class);
        delegate.test();
        expectLastCall().andThrow(new CommunicationException(new ErrorMessage(1, "Expected error."))).times(1 + 3);

        replay(delegate);

        try {
            Delegate proxy = BackoffFailureHandler.newInstance(delegate, Delegate.class, 3, 1000);
            proxy.test();
        } finally {
            verify(delegate);
        }
    }

    @Test()
    public void testProxyPerf() throws Exception {
        int warmIterations = 1000;
        int testIterations = 100000;

        Delegate delegate = createMock(Delegate.class);
        delegate.test();
        expectLastCall().andReturn("test").times(warmIterations + testIterations);

        replay(delegate);

        try {
            Delegate proxy = BackoffFailureHandler.newInstance(delegate, Delegate.class, 1, 1000);

            System.out.println("testProxyPerf running...");

            System.out.println("Warming up ...");
            for (int i = 0; i < warmIterations; i++) {
                proxy.test();
            }

            System.out.println("Testing ...");
            long start = System.nanoTime();
            for (int i = 0; i < testIterations; i++) {
                proxy.test();
            }
            long elapsed = System.nanoTime() - start;
            System.out.println("Elapsed: " + TimeUnit.MILLISECONDS.convert(elapsed, TimeUnit.NANOSECONDS));
        } finally {
            verify(delegate);
        }
    }

    public static interface Delegate {

        @HandleFailure(exception = CommunicationException.class)
        public String test() throws Exception;
    }
}
