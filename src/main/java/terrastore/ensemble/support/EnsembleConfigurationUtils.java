package terrastore.ensemble.support;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.ensemble.EnsembleConfiguration;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfigurationUtils {

    public static EnsembleConfiguration makeDefault(String clusterName) {
        EnsembleConfiguration configuration = new EnsembleConfiguration();
        configuration.setLocalCluster(clusterName);
        configuration.setClusters(Arrays.asList(clusterName));
        configuration.setDiscoveryInterval(1);
        return configuration;
    }

    public static EnsembleConfiguration readFrom(InputStream stream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(stream, EnsembleConfiguration.class);
    }
}
