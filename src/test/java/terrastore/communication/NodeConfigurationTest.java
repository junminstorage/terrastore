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
