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
package terrastore.store.impl;

import terrastore.util.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;
import terrastore.event.Event;
import terrastore.event.EventBus;
import terrastore.event.impl.ValueChangedEvent;
import terrastore.event.impl.ValueRemovedEvent;
import terrastore.internal.tc.TCMaster;
import terrastore.store.Key;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Update;
import terrastore.store.operators.Function;
import terrastore.store.Value;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class TCBucketEventingTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String JSON_VALUE_2 = "{\"test\":\"test2\"}";
    private static final String JSON_UPDATED = "{\"test\":\"value1\"}";
    private TCBucket bucket;

    @Before
    public void setUp() {
        TCMaster.getInstance().connect("localhost:9510", 1, TimeUnit.SECONDS);
        bucket = new TCBucket("bucket");
    }

    @Test
    public void testPutFiresEventBus() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());

        Capture<Event> capturedEvent = new Capture<Event>();
        EventBus eventBus = createMock(EventBus.class);
        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent));
        expectLastCall().once();

        replay(eventBus);

        bucket.setEventBus(eventBus);
        bucket.put(key, value);
        assertEquals(value, bucket.get(key));

        assertEquals(ValueChangedEvent.class, capturedEvent.getValue().getClass());
        assertEquals("bucket", capturedEvent.getValue().getBucket());
        assertEquals("key", capturedEvent.getValue().getKey());
        assertNull(capturedEvent.getValue().getOldValueAsBytes());
        assertArrayEquals(JSON_VALUE.getBytes(), capturedEvent.getValue().getNewValueAsBytes());
        assertEquals("test", capturedEvent.getValue().getNewValueAsMap().get("test"));

        verify(eventBus);
    }

    @Test
    public void testPutDoesNotFireEventBusIfNotEnabled() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());

        EventBus eventBus = createMock(EventBus.class);
        eventBus.isEnabled();
        expectLastCall().andReturn(false);

        replay(eventBus);

        bucket.setEventBus(eventBus);
        bucket.put(key, value);

        verify(eventBus);
    }

    @Test
    public void testPutAndPutAgainFireEventBus() throws StoreOperationException {
        Key key = new Key("key");
        Value value1 = new Value(JSON_VALUE.getBytes());
        Value value2 = new Value(JSON_VALUE_2.getBytes());

        Capture<Event> capturedEvent1 = new Capture<Event>();
        Capture<Event> capturedEvent2 = new Capture<Event>();
        EventBus eventBus = createMock(EventBus.class);

        bucket.setEventBus(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent1));
        expectLastCall().once();

        replay(eventBus);

        bucket.put(key, value1);
        assertEquals(value1, bucket.get(key));
        assertEquals(ValueChangedEvent.class, capturedEvent1.getValue().getClass());
        assertEquals("bucket", capturedEvent1.getValue().getBucket());
        assertEquals("key", capturedEvent1.getValue().getKey());
        assertNull(capturedEvent1.getValue().getOldValueAsBytes());
        assertEquals("test", capturedEvent1.getValue().getNewValueAsMap().get("test"));
        assertArrayEquals(JSON_VALUE.getBytes(), capturedEvent1.getValue().getNewValueAsBytes());

        verify(eventBus);
        reset(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent2));
        expectLastCall().once();

        replay(eventBus);

        bucket.put(key, value2);
        assertEquals(value2, bucket.get(key));
        assertEquals(ValueChangedEvent.class, capturedEvent2.getValue().getClass());
        assertEquals("bucket", capturedEvent2.getValue().getBucket());
        assertEquals("key", capturedEvent2.getValue().getKey());
        assertEquals("test", capturedEvent2.getValue().getOldValueAsMap().get("test"));
        assertArrayEquals(JSON_VALUE.getBytes(), capturedEvent2.getValue().getOldValueAsBytes());
        assertEquals("test2", capturedEvent2.getValue().getNewValueAsMap().get("test"));
        assertArrayEquals(JSON_VALUE_2.getBytes(), capturedEvent2.getValue().getNewValueAsBytes());

        verify(eventBus);
    }

    @Test
    public void testPutAndRemoveFireEventBus() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());

        Capture<Event> capturedEvent1 = new Capture<Event>();
        Capture<Event> capturedEvent2 = new Capture<Event>();
        EventBus eventBus = createMock(EventBus.class);

        bucket.setEventBus(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent1));
        expectLastCall().once();

        replay(eventBus);

        bucket.put(key, value);
        assertEquals(value, bucket.get(key));

        verify(eventBus);
        reset(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent2));
        expectLastCall().once();

        replay(eventBus);

        bucket.remove(key);
        assertFalse(bucket.keys().contains(key));
        assertEquals(ValueRemovedEvent.class, capturedEvent2.getValue().getClass());
        assertEquals("bucket", capturedEvent2.getValue().getBucket());
        assertEquals("key", capturedEvent2.getValue().getKey());
        assertArrayEquals(JSON_VALUE.getBytes(), capturedEvent2.getValue().getOldValueAsBytes());
        assertEquals("test", capturedEvent2.getValue().getOldValueAsMap().get("test"));
        assertNull(capturedEvent2.getValue().getNewValueAsBytes());

        verify(eventBus);
    }

    @Test
    public void testPutAndUpdateFireEventBus() throws StoreOperationException {
        Key key = new Key("key");
        Value value = new Value(JSON_VALUE.getBytes());
        Value updated = new Value(JSON_UPDATED.getBytes());
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("p1", "value1");
        Update update = new Update("function", 3000, params);
        Function function = new Function() {

            @Override
            public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters) {
                value.put("test", parameters.get("p1"));
                return value;
            }

        };

        bucket.setFunctions(Maps.hash(new String[]{"function"}, new Function[]{function}));

        Capture<Event> capturedEvent1 = new Capture<Event>();
        Capture<Event> capturedEvent2 = new Capture<Event>();
        EventBus eventBus = createMock(EventBus.class);

        bucket.setEventBus(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent1));
        expectLastCall().once();

        replay(eventBus);

        bucket.put(key, value);
        assertEquals(value, bucket.get(key));

        verify(eventBus);
        reset(eventBus);

        eventBus.isEnabled();
        expectLastCall().andReturn(true);
        eventBus.publish(capture(capturedEvent2));
        expectLastCall().once();

        replay(eventBus);

        bucket.update(key, update);
        assertEquals(updated, bucket.get(key));

        assertEquals(ValueChangedEvent.class, capturedEvent2.getValue().getClass());
        assertEquals("bucket", capturedEvent2.getValue().getBucket());
        assertEquals("key", capturedEvent2.getValue().getKey());
        assertArrayEquals(JSON_VALUE.getBytes(), capturedEvent2.getValue().getOldValueAsBytes());
        assertEquals("test", capturedEvent2.getValue().getOldValueAsMap().get("test"));
        assertArrayEquals(JSON_UPDATED.getBytes(), capturedEvent2.getValue().getNewValueAsBytes());
        assertEquals("value1", capturedEvent2.getValue().getNewValueAsMap().get("test"));

        verify(eventBus);
    }

}
