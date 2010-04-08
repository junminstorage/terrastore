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