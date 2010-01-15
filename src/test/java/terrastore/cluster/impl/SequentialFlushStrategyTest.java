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
package terrastore.cluster.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import terrastore.cluster.FlushCondition;
import terrastore.cluster.FlushStrategy;
import terrastore.store.Bucket;
import terrastore.store.Store;
import static org.easymock.classextension.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class SequentialFlushStrategyTest {

    @Test
    public void testSequentiallyFlushAllKeys() {
        FlushCondition condition = createMock(FlushCondition.class);
        Store store = createMock(Store.class);
        Bucket bucket1 = createMock(Bucket.class);
        Bucket bucket2 = createMock(Bucket.class);
        Set<String> keys1 = Sets.newHashSet("k1");
        Set<String> keys2 = Sets.newHashSet("k2");

        condition.isSatisfied(bucket1, "k1");
        expectLastCall().andReturn(true).once();
        condition.isSatisfied(bucket2, "k2");
        expectLastCall().andReturn(true).once();
        store.buckets();
        expectLastCall().andReturn(Lists.newArrayList(bucket1, bucket2)).once();
        bucket1.getName();
        expectLastCall().andReturn("b1").once();
        bucket2.getName();
        expectLastCall().andReturn("b2").once();
        bucket1.keys();
        expectLastCall().andReturn(keys1).once();
        bucket2.keys();
        expectLastCall().andReturn(keys2).once();
        bucket1.flush(keys1);
        expectLastCall().once();
        bucket2.flush(keys2);
        expectLastCall().once();

        replay(condition, store, bucket1, bucket2);

        FlushStrategy strategy = new SequentialFlushStrategy();
        strategy.flush(store, condition);

        verify(condition, store, bucket1, bucket2);
    }

    @Test
    public void testSequentiallyFlushNoKeys() {
        FlushCondition condition = createMock(FlushCondition.class);
        Store store = createMock(Store.class);
        Bucket bucket1 = createMock(Bucket.class);
        Bucket bucket2 = createMock(Bucket.class);
        Set<String> keys1 = Sets.newHashSet("k1");
        Set<String> keys2 = Sets.newHashSet("k2");

        condition.isSatisfied(bucket1, "k1");
        expectLastCall().andReturn(false).once();
        condition.isSatisfied(bucket2, "k2");
        expectLastCall().andReturn(false).once();
        store.buckets();
        expectLastCall().andReturn(Lists.newArrayList(bucket1, bucket2)).once();
        bucket1.getName();
        expectLastCall().andReturn("b1").once();
        bucket2.getName();
        expectLastCall().andReturn("b2").once();
        bucket1.keys();
        expectLastCall().andReturn(keys1).once();
        bucket2.keys();
        expectLastCall().andReturn(keys2).once();
        bucket1.flush(eq(new HashSet(0)));
        expectLastCall().once();
        bucket2.flush(eq(new HashSet(0)));
        expectLastCall().once();

        replay(condition, store, bucket1, bucket2);

        FlushStrategy strategy = new SequentialFlushStrategy();
        strategy.flush(store, condition);

        verify(condition, store, bucket1, bucket2);
    }
}