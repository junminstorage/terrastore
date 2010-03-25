package terrastore.ensemble.support;

import java.io.IOException;
import java.io.InputStream;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.ensemble.EnsembleConfiguration;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfigurationUtils {

    public static EnsembleConfiguration read(InputStream stream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(stream, EnsembleConfiguration.class);
    }
}
