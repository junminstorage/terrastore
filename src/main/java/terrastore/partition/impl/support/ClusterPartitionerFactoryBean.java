package terrastore.partition.impl.support;

import java.util.Map;
import org.springframework.beans.factory.FactoryBean;
import terrastore.partition.ClusterPartitioner;
import terrastore.partition.CustomClusterPartitionerStrategy;
import terrastore.partition.impl.ClusterCustomPartitioner;
import terrastore.util.annotation.AnnotationScanner;

/**
 * @author Sergio Bossa
 */
public class ClusterPartitionerFactoryBean implements FactoryBean {

    private final ClusterPartitioner defaultPartitioner;
    private final ClusterPartitioner customPartitioner;

    public ClusterPartitionerFactoryBean(ClusterPartitioner defaultPartitioner, AnnotationScanner annotationScanner) {
        this.defaultPartitioner = defaultPartitioner;
        this.customPartitioner = scanForCustomPartitioner(annotationScanner);
    }

    @Override
    public Object getObject() throws Exception {
        if (customPartitioner != null) {
            return customPartitioner;
        } else {
            return defaultPartitioner;
        }
    }

    @Override
    public Class getObjectType() {
        return ClusterPartitioner.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private ClusterPartitioner scanForCustomPartitioner(AnnotationScanner annotationScanner) {
        Map strategies = annotationScanner.scanByType(CustomClusterPartitionerStrategy.class);
        if (strategies.size() == 1) {
            CustomClusterPartitionerStrategy strategy = (CustomClusterPartitionerStrategy) strategies.values().iterator().next();
            return new ClusterCustomPartitioner(strategy);
        } else if (strategies.size() == 0) {
            return null;
        } else {
            throw new IllegalStateException("More than one class found of type: " + CustomClusterPartitionerStrategy.class);
        }
    }
}
