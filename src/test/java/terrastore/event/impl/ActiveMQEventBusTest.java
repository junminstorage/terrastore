package terrastore.event.impl;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.broker.BrokerService;
import org.easymock.IAnswer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.event.EventListener;
import terrastore.event.ValueChangedEvent;
import terrastore.event.ValueRemovedEvent;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ActiveMQEventBusTest {

    private static final BrokerService broker = new BrokerService();

    @BeforeClass
    public static void setUpClass() throws Exception {
        broker.addConnector("vm://localhost");
        broker.setPersistent(false);
        broker.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        broker.stop();
    }

    @Test
    public void testValueChangedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.init();
        expectLastCall().once();
        expect(listener.observes("bucket")).andReturn(true).once();
        listener.onValueChanged(eq("bucket"), eq("key"), aryEq("value".getBytes()));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener);

        ActiveMQEventBus bus = null;
        try {
            bus = new ActiveMQEventBus(Arrays.asList(listener), "vm://localhost");
            bus.publish(new ValueChangedEvent("bucket", "key", "value".getBytes()));

            listenerLatch.await(3, TimeUnit.SECONDS);

            verify(listener);
        } finally {
            bus.shutdown();
        }
    }

    @Test
    public void testValueRemovedEvent() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.init();
        expectLastCall().once();
        expect(listener.observes("bucket")).andReturn(true).once();
        listener.onValueRemoved(eq("bucket"), eq("key"));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener);

        ActiveMQEventBus bus = null;
        try {
            bus = new ActiveMQEventBus(Arrays.asList(listener), "vm://localhost");
            bus.publish(new ValueRemovedEvent("bucket", "key"));

            listenerLatch.await(3, TimeUnit.SECONDS);

            verify(listener);
        } finally {
            bus.shutdown();
        }
    }

    @Test
    public void testExceptionOnEventProcessingCausesRedelivery() throws Exception {
        final CountDownLatch listenerLatch = new CountDownLatch(1);
        EventListener listener = createMock(EventListener.class);
        makeThreadSafe(listener, true);
        listener.init();
        expectLastCall().once();
        listener.observes("bucket");
        expectLastCall().andReturn(true).times(2);
        listener.onValueChanged(eq("bucket"), eq("key"), aryEq("value".getBytes()));
        expectLastCall().andThrow(new RuntimeException()).once().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                listenerLatch.countDown();
                return null;
            }
        }).once();

        replay(listener);

        ActiveMQEventBus bus = null;
        try {
            bus = new ActiveMQEventBus(Arrays.asList(listener), "vm://localhost");
            bus.publish(new ValueChangedEvent("bucket", "key", "value".getBytes()));

            listenerLatch.await(3, TimeUnit.SECONDS);

            verify(listener);
        } finally {
            bus.shutdown();
        }
    }
}
