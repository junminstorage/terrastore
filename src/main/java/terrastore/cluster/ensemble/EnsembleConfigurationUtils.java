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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.codehaus.jackson.map.ObjectMapper;
import terrastore.cluster.ensemble.EnsembleConfiguration;

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
