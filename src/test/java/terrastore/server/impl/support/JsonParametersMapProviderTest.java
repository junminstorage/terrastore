/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
package terrastore.server.impl.support;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import terrastore.server.Parameters;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonParametersMapProviderTest {

    private static final String JSON_PARAMETERS = "{\"test1\":\"test1\"}";

    @Test
    public void testRead() throws Exception {
        JsonParametersMapProvider provider = new JsonParametersMapProvider();
        Map<String, String> map = new HashMap<String, String>();
        map.put("test1", "test1");

        Parameters parameters = provider.readFrom(Parameters.class, null, null, null, null, new ByteArrayInputStream(JSON_PARAMETERS.getBytes()));

        assertEquals(1, parameters.size());
        assertEquals("test1", parameters.get("test1"));
    }
}
