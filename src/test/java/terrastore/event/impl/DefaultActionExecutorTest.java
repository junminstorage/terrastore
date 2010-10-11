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
package terrastore.event.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.easymock.EasyMock;
import org.junit.Test;
import terrastore.event.Action;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.util.collect.Maps;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class DefaultActionExecutorTest {

    @Test
    public void testSubmitPutAction() throws Exception {
        String bucket = "bucket";
        String key = "key";
        String value = "{\"key\":\"value\"}";

        UpdateService service = createMock(UpdateService.class);
        service.putValue(eq(bucket), eq(new Key(key)), eq(new Value(value.getBytes())), EasyMock.<Predicate>isNull());
        expectLastCall().once();

        replay(service);

        DefaultActionExecutor executor = new DefaultActionExecutor(service);
        Action action = executor.makePutAction(bucket, key, Maps.<String, Object>hash(new String[]{"key"}, new String[]{"value"}));
        executor.submit(action).get(60, TimeUnit.SECONDS);

        verify(service);
    }

    @Test
    public void testSubmitPutActionWithPredicate() throws Exception {
        String bucket = "bucket";
        String key = "key";
        String value = "{\"key\":\"value\"}";
        String predicate = "test:test";

        UpdateService service = createMock(UpdateService.class);
        service.putValue(eq(bucket), eq(new Key(key)), eq(new Value(value.getBytes())), EasyMock.<Predicate>notNull());
        expectLastCall().once();

        replay(service);

        DefaultActionExecutor executor = new DefaultActionExecutor(service);
        Action action = executor.makePutAction(bucket, key, Maps.<String, Object>hash(new String[]{"key"}, new String[]{"value"}), predicate);
        executor.submit(action).get(60, TimeUnit.SECONDS);

        verify(service);
    }

    @Test
    public void testSubmitRemoveAction() throws Exception {
        String bucket = "bucket";
        String key = "key";

        UpdateService service = createMock(UpdateService.class);
        service.removeValue(eq(bucket), eq(new Key(key)));
        expectLastCall().once();

        replay(service);

        DefaultActionExecutor executor = new DefaultActionExecutor(service);
        Action action = executor.makeRemoveAction(bucket, key);
        executor.submit(action).get(60, TimeUnit.SECONDS);

        verify(service);
    }

    @Test
    public void testSubmitUpdateAction() throws Exception {
        String bucket = "bucket";
        String key = "key";
        String function = "function";
        long timeout = 1000;

        UpdateService service = createMock(UpdateService.class);
        service.updateValue(eq(bucket), eq(new Key(key)), eq(new Update(function, timeout, Maps.<String, Object>hash(new String[]{"key"}, new String[]{"value"}))));
        expectLastCall().andReturn(null).once();

        replay(service);

        DefaultActionExecutor executor = new DefaultActionExecutor(service);
        Action action = executor.makeUpdateAction(bucket, key, function, timeout, Maps.<String, Object>hash(new String[]{"key"}, new String[]{"value"}));
        executor.submit(action).get(60, TimeUnit.SECONDS);

        verify(service);
    }

    @Test(expected = RuntimeException.class)
    public void testSubmitActionAndThrowException() throws Throwable {
        UpdateService service = null;
        try {
            String bucket = "bucket";
            String key = "key";
            String value = "{\"key\":\"value\"}";

            service = createMock(UpdateService.class);
            service.putValue(eq(bucket), eq(new Key(key)), eq(new Value(value.getBytes())), EasyMock.<Predicate>isNull());
            expectLastCall().andThrow(new RuntimeException("Expected!")).once();

            replay(service);

            DefaultActionExecutor executor = new DefaultActionExecutor(service);
            Action action = executor.makePutAction(bucket, key, Maps.<String, Object>hash(new String[]{"key"}, new String[]{"value"}));
            executor.submit(action).get(60, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            throw ex.getCause();
        } finally {
            verify(service);
        }
    }

}
