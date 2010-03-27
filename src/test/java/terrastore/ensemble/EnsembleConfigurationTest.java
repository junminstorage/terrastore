package terrastore.ensemble;

import terrastore.ensemble.EnsembleConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class EnsembleConfigurationTest {

    private final String CONFIGURATION = "{\"clusters\":{\"cluster1\":[{\"host\":\"localhost\",\"port\":6000}]}}";

    @Test
    public void testSomeMethod() throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        EnsembleConfiguration configuration = jsonMapper.readValue(new ByteArrayInputStream(CONFIGURATION.getBytes()), EnsembleConfiguration.class);
        assertNotNull(configuration);
    }

}