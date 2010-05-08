package terrastore.partition.impl.support;

import org.junit.Test;
import terrastore.annotation.AutoDetect;
import terrastore.partition.ClusterPartitioner;
import terrastore.partition.CustomClusterPartitionerStrategy;
import terrastore.partition.impl.ClusterCustomPartitioner;
import terrastore.util.annotation.AnnotationScanner;
import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

/**
 * @author Sergio Bossa
 */
public class ClusterPartitionerFactoryBeanTest {

    @Test
    public void testGetObject() throws Exception {
        ClusterPartitioner defaultPartitioner = createMock(ClusterPartitioner.class);

        ClusterPartitionerFactoryBean factory = new ClusterPartitionerFactoryBean(defaultPartitioner, new AnnotationScanner());
        assertEquals(ClusterCustomPartitioner.class, factory.getObject().getClass());
    }

    @AutoDetect(name = "dummy")
    public static class DummyStrategy implements CustomClusterPartitionerStrategy {

        @Override
        public Node getNodeFor(String cluster, String bucket) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Node getNodeFor(String cluster, String bucket, String key) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
