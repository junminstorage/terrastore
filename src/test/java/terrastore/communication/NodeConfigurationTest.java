package terrastore.communication;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class NodeConfigurationTest {

    @Test
    public void testPublishHostIsSameAsBindHostGuessing() {
        NodeConfiguration configuration = new NodeConfiguration("name", "127.0.0.1", 6000, "127.0.0.1", 8000);
        assertEquals("127.0.0.1", configuration.getNodeBindHost());
        assertEquals(1, configuration.getNodePublishHosts().size());
        assertEquals(configuration.getNodeBindHost(), configuration.getNodePublishHosts().iterator().next());
    }

    @Test
    public void testPublishHostsWhenBindingOnAnyHost() {
        NodeConfiguration configuration = new NodeConfiguration("name", "0.0.0.0", 6000, "127.0.0.1", 8000);
        assertEquals("0.0.0.0", configuration.getNodeBindHost());
        assertNotNull(configuration.getNodePublishHosts());
        for (String host : configuration.getNodePublishHosts()) {
            System.out.println(host);
        }
    }

}
