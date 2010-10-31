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
