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

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Test;
import terrastore.server.Buckets;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonBucketsProviderTest {

    private static final String BUCKETS = "[\"1\",\"2\"]";

    @Test
    public void testWrite() throws Exception {
        JsonBucketsProvider provider = new JsonBucketsProvider();
        Set<String> names = new LinkedHashSet<String>();
        names.add("1");
        names.add("2");

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        provider.writeTo(new Buckets(names), null, null, null, null, null, stream);

        assertEquals(new String(BUCKETS.getBytes(), "UTF-8"), new String(stream.toByteArray(), "UTF-8"));
    }
}
