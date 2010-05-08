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
