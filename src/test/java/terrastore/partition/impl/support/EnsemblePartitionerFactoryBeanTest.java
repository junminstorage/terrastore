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
package terrastore.partition.impl.support;

import org.junit.Test;
import terrastore.annotation.AutoDetect;
import terrastore.partition.CustomEnsemblePartitionerStrategy;
import terrastore.partition.EnsemblePartitioner;
import terrastore.partition.impl.EnsembleCustomPartitioner;
import terrastore.util.annotation.AnnotationScanner;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class EnsemblePartitionerFactoryBeanTest {

    @Test
    public void testGetObject() throws Exception {
        EnsemblePartitioner defaultPartitioner = createMock(EnsemblePartitioner.class);

        EnsemblePartitionerFactoryBean factory = new EnsemblePartitionerFactoryBean(defaultPartitioner, new AnnotationScanner());
        assertEquals(EnsembleCustomPartitioner.class, factory.getObject().getClass());
    }

    @AutoDetect(name = "dummy")
    public static class DummyStrategy implements CustomEnsemblePartitionerStrategy {

        @Override
        public Cluster getClusterFor(String bucket) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Cluster getClusterFor(String bucket, String key) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
