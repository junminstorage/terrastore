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
package terrastore.communication.local;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.communication.protocol.Command;
import terrastore.router.Router;
import terrastore.store.Store;
import static org.junit.Assert.*;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class LocalCommunicationTest {

    @Test
    public void testSynchronousCommunication() throws Exception {
        Object result = new Object();

        Router router = createMock(Router.class);
        Store store = createMock(Store.class);
        Command command = createMock(Command.class);

        command.executeOn(store);
        expectLastCall().andReturn(result).once();

        replay(router, store, command);

        LocalProcessor processor = new LocalProcessor(router, store);
        LocalNode node = new LocalNode(new ServerConfiguration("node", "localhost", 6000, "localhost", 8000), processor);
        assertEquals(result, node.send(command));

        verify(router, store, command);
    }

    @Test
    public void testSynchronousCommunicationOnPauseCausesRouting() throws Exception {
        final Object expected = new Object();

        Router router = createMock(Router.class);
        Store store = createMock(Store.class);
        final Command command = createMock(Command.class);

        command.executeOn(router);
        expectLastCall().andReturn(expected).once();

        replay(router, store, command);

        final LocalProcessor processor = new LocalProcessor(router, store);
        final LocalNode node = new LocalNode(new ServerConfiguration("node", "localhost", 6000, "localhost", 8000), processor);
        final CountDownLatch success = new CountDownLatch(1);

        processor.pause();
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    assertEquals(expected, node.send(command));
                    success.countDown();
                } catch (Exception ex) {
                }
            }
        }).start();
        Thread.sleep(1000);
        processor.resume();
        success.await(60, TimeUnit.SECONDS);

        verify(router, store, command);
    }
}
