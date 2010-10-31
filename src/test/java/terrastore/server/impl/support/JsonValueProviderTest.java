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
package terrastore.server.impl.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.Test;
import terrastore.store.Value;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonValueProviderTest {

    private static final String JSON_VALUE = "{\"test1\" : \"test\", \"test2\" : [1, {\"test1\":\"test\"}], \"test3\" : {\"test1\":\"test\"}}";

    @Test
    public void testRead() throws Exception {
        JsonValueProvider provider = new JsonValueProvider();

        ByteArrayInputStream stream = new ByteArrayInputStream(JSON_VALUE.getBytes());
        Value value = provider.readFrom(null, null, null, null, null, stream);

        assertArrayEquals(JSON_VALUE.getBytes(), value.getBytes());
    }

    @Test
    public void testWrite() throws Exception {
        JsonValueProvider provider = new JsonValueProvider();

        Value value = new Value(JSON_VALUE.getBytes("UTF-8"));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        provider.writeTo(value, null, null, null, null, null, stream);

        assertArrayEquals(JSON_VALUE.getBytes(), stream.toByteArray());
    }

}
