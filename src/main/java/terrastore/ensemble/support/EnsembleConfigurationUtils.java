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

    public static EnsembleConfiguration makeDefault(String ensembleName, String clusterName) {
        EnsembleConfiguration configuration = new EnsembleConfiguration();
        configuration.setEnsembleName(ensembleName);
        configuration.setClusterName(clusterName);
        configuration.setClusters(Arrays.asList(clusterName));
        return configuration;
    }

    public static EnsembleConfiguration readFrom(InputStream stream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(stream, EnsembleConfiguration.class);
    }
}
