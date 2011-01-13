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
package terrastore.common;

import java.util.HashSet;
import org.junit.Test;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ClusterStatsTest {

    @Test
    public void testClusterIsAvailableWithNodes() {
        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", Sets.linked(new ClusterStats.Node("node-1", "localhost", 8080)));
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));
        assertEquals(ClusterStats.Status.AVAILABLE, clusterStats.getClusters().iterator().next().getStatus());
    }

    @Test
    public void testClusterIsAvailableWithNoNodes() {
        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", new HashSet<ClusterStats.Node>());
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));
        assertEquals(ClusterStats.Status.UNAVAILABLE, clusterStats.getClusters().iterator().next().getStatus());
    }
}