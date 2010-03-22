package terrastore.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfiguration {

    public String local;
    public Map<String, NodeInfo> clusters;

    public EnsembleConfiguration(String local) {
        this.local = local;
        clusters = new HashMap<String, NodeInfo>(0);
    }

    public EnsembleConfiguration() {
    }
    
    public static EnsembleConfiguration read(InputStream stream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.readValue(stream, EnsembleConfiguration.class);
    }

    public static class NodeInfo {

        public String host;
        public int port;
    }
}
