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
package terrastore.cluster.ensemble;

import java.io.ByteArrayInputStream;
import org.junit.Test;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfigurationTest {

    private final String CONFIGURATION = "{"
            + "\"localCluster\":\"cluster1\","
            + "\"discoveryInterval\":1000,"
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_CLUSTER_CONFIGURATION = "{"
            + "\"localCluster\":\"cluster1\","
            + "\"discoveryInterval\":1000,"
            + "\"clusters\":[\"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_SEED_CONFIGURATION = "{"
            + "\"localCluster\":\"cluster1\","
            + "\"discoveryInterval\":1000,"
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster3\":\"www.acme3.org:6000\"}}";
    private final String BAD_SEED_CONFIGURATION = "{"
            + "\"localCluster\":\"cluster1\","
            + "\"discoveryInterval\":1000,"
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org\", \"cluster3\":\"www.acme3.org:6000\"}}";

    @Test
    public void testDefaultConfiguration() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfiguration.makeDefault("cluster");
        configuration.validate();
    }

    @Test
    public void testCorrectConfiguration() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithMissingCluster() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_CLUSTER_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithMissingSeed() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_SEED_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithBadSeed() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(BAD_SEED_CONFIGURATION.getBytes()));
        configuration.validate();
    }
}
