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

    private final String FIXED_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"fixed\", \"interval\":\"1000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String ADAPTIVE_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"adaptive\", \"interval\":\"1000\", \"baseline\":\"25000\", \"upboundIncrement\":\"5000\", \"upboundLimit\":\"60000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String WRONG_FIXED_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"fixed\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_INTERVAL_ADAPTIVE_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"adaptive\", \"baseline\":\"25000\", , \"upboundIncrement\":\"5000\", \"upboundLimit\":\"60000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_BASELINE_ADAPTIVE_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"adaptive\", \"interval\":\"1000\", , \"upboundIncrement\":\"5000\", \"upboundLimit\":\"60000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_INCREMENT_ADAPTIVE_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"adaptive\", \"interval\":\"1000\", \"baseline\":\"25000\", \"upboundLimit\":\"60000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_LIMIT_ADAPTIVE_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"adaptive\", \"interval\":\"1000\", \"baseline\":\"25000\", \"upboundIncrement\":\"5000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_CLUSTER_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"fixed\", \"interval\":\"1000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org:6000\", \"cluster3\":\"www.acme3.org:6000\"}}";
    private final String MISSING_SEED_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"fixed\", \"interval\":\"1000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster3\":\"www.acme3.org:6000\"}}";
    private final String BAD_SEED_CONFIGURATION = "{"
            + "\"discovery\":{\"type\":\"fixed\", \"interval\":\"1000\"},"
            + "\"localCluster\":\"cluster1\","
            + "\"clusters\":[\"cluster1\", \"cluster2\", \"cluster3\"],"
            + "\"seeds\":{\"cluster2\":\"www.acme2.org\", \"cluster3\":\"www.acme3.org:6000\"}}";

    @Test
    public void testDefaultConfiguration() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfiguration.makeDefault("cluster");
        configuration.validate();
    }

    @Test
    public void testFixedSchedulerConfiguration() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(FIXED_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test
    public void testAdaptiveSchedulerConfiguration() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(ADAPTIVE_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testFixedConfigurationWithMissingInterval() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(WRONG_FIXED_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testAdaptiveConfigurationWithMissingInterval() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_INTERVAL_ADAPTIVE_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testAdaptiveConfigurationWithMissingBaseline() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_BASELINE_ADAPTIVE_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testAdaptiveConfigurationWithMissingIncrement() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_INCREMENT_ADAPTIVE_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testAdaptiveConfigurationWithMissingLimit() throws Exception {
        EnsembleConfiguration configuration = JsonUtils.readEnsembleConfiguration(new ByteArrayInputStream(MISSING_LIMIT_ADAPTIVE_CONFIGURATION.getBytes()));
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
