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
package terrastore.util.collect;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SerializingMapTest {

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        Map<String, String> source = new TreeMap<String, String>();
        source.put("c", "3");
        source.put("b", "2");
        source.put("a", "1");

        SerializingMap<String, String> serializingMap = new SerializingMap<String, String>(source);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(serializingMap);

        ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        SerializingMap<String, String> read = (SerializingMap<String, String>) input.readObject();
        assertEquals(3, read.size());
        assertEquals("a", read.keySet().toArray()[0]);
        assertEquals("b", read.keySet().toArray()[1]);
        assertEquals("c", read.keySet().toArray()[2]);
        assertEquals("1", read.values().toArray()[0]);
        assertEquals("2", read.values().toArray()[1]);
        assertEquals("3", read.values().toArray()[2]);
    }
}