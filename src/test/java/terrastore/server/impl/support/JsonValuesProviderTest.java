/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.junit.Test;
import terrastore.server.Values;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.Value;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonValuesProviderTest {

    private static final String JSON_VALUE_1 = "{\"test1\":\"test1\"}";
    private static final String JSON_VALUE_2 = "{\"test2\":\"test2\"}";
    private static final String JSON_VALUES = "{\"key1\":{\"test1\":\"test1\"},\"key2\":{\"test2\":\"test2\"}}";
    private static final String JSON_BAD_VALUES = "{\"key\":\"test\"}";

    @Test
    public void testWrite() throws Exception {
        JsonValuesProvider provider = new JsonValuesProvider();
        Map<Key, Value> map = new TreeMap<Key, Value>();
        map.put(new Key("key1"), new Value(JSON_VALUE_1.getBytes("UTF-8")));
        map.put(new Key("key2"), new Value(JSON_VALUE_2.getBytes("UTF-8")));

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        provider.writeTo(new Values(map), null, null, null, null, null, stream);

        assertEquals(new String(JSON_VALUES.getBytes(), "UTF-8"), new String(stream.toByteArray(), "UTF-8"));
    }

    @Test
    public void testRead() throws Exception {
        ByteArrayInputStream stream = new ByteArrayInputStream(JSON_VALUES.getBytes());
        JsonValuesProvider provider = new JsonValuesProvider();
        Values values = provider.readFrom(null, null, null, null, null, stream);
        assertEquals(2, values.size());
    }

    @Test(expected = WebApplicationException.class)
    public void testBadRead() throws Exception {
        ByteArrayInputStream stream = new ByteArrayInputStream(JSON_BAD_VALUES.getBytes());
        JsonValuesProvider provider = new JsonValuesProvider();
        provider.readFrom(null, null, null, null, null, stream);
    }

}
