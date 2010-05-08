package terrastore.partition.impl.support;

import java.util.Map;
import org.springframework.beans.factory.FactoryBean;
import terrastore.partition.CustomEnsemblePartitionerStrategy;
import terrastore.partition.EnsemblePartitioner;
import terrastore.partition.impl.EnsembleCustomPartitioner;
import terrastore.util.annotation.AnnotationScanner;

/**
 * @author Sergio Bossa
 */
public class EnsemblePartitionerFactoryBean implements FactoryBean {

    private final EnsemblePartitioner defaultPartitioner;
    private final EnsemblePartitioner customPartitioner;

    public EnsemblePartitionerFactoryBean(EnsemblePartitioner defaultPartitioner, AnnotationScanner annotationScanner) {
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
        return EnsemblePartitioner.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private EnsemblePartitioner scanForCustomPartitioner(AnnotationScanner annotationScanner) {
        Map strategies = annotationScanner.scanByType(CustomEnsemblePartitionerStrategy.class);
        if (strategies.size() == 1) {
            CustomEnsemblePartitionerStrategy strategy = (CustomEnsemblePartitionerStrategy) strategies.values().iterator().next();
            return new EnsembleCustomPartitioner(strategy);
        } else if (strategies.size() == 0) {
            return null;
        } else {
            throw new IllegalStateException("More than one class found of type: " + CustomEnsemblePartitionerStrategy.class);
        }
    }
}
