package terrastore.ensemble;

import java.io.ByteArrayInputStream;
import org.junit.Test;
import terrastore.ensemble.support.EnsembleConfigurationUtils;

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
        EnsembleConfiguration configuration = EnsembleConfigurationUtils.makeDefault("cluster");
        configuration.validate();
    }

    @Test
    public void testCorrectConfiguration() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfigurationUtils.readFrom(new ByteArrayInputStream(CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithMissingCluster() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfigurationUtils.readFrom(new ByteArrayInputStream(MISSING_CLUSTER_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithMissingSeed() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfigurationUtils.readFrom(new ByteArrayInputStream(MISSING_SEED_CONFIGURATION.getBytes()));
        configuration.validate();
    }

    @Test(expected = EnsembleConfigurationException.class)
    public void testConfigurationWithBadSeed() throws Exception {
        EnsembleConfiguration configuration = EnsembleConfigurationUtils.readFrom(new ByteArrayInputStream(BAD_SEED_CONFIGURATION.getBytes()));
        configuration.validate();
    }
}
