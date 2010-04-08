package terrastore.util.collect;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class SerializingSetTest {

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        Set<String> source = new TreeSet<String>();
        source.add("c");
        source.add("b");
        source.add("a");

        SerializingSet<String> serializingSet = new SerializingSet<String>(source);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(serializingSet);

        ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        SerializingSet<String> read = (SerializingSet<String>) input.readObject();
        assertEquals(3, read.size());
        assertEquals("a", read.toArray()[0]);
        assertEquals("b", read.toArray()[1]);
        assertEquals("c", read.toArray()[2]);
    }
}