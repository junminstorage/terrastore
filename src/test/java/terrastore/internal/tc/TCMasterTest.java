package terrastore.internal.tc;

import terrastore.internal.tc.TCMaster;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.cache.serialization.SerializationStrategy;
import org.terracotta.cache.serialization.SerializedEntry;
import org.terracotta.collections.ClusteredMap;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TCMasterTest {

    @Test
    public void testConnect() {
        TCMaster master = TCMaster.getInstance();
        assertFalse(master.connect("localhost:9510", 1, TimeUnit.SECONDS));
    }

    /*@Test
    public void test() throws IOException, ClassNotFoundException {
        TCMaster master1 = new TCMaster();
        //TCMaster master2 = new TCMaster();
        assertTrue(master1.connect("localhost:9510", 1, TimeUnit.MINUTES));
        //assertTrue(master2.connect("localhost:9510", 1, TimeUnit.MINUTES));

        ClusteredMap<String, SerializedEntry<StringWrapper>> map1 = master1.getMap("map");

        System.out.println(map1.getAllLocalEntriesSnapshot().size());
        System.out.println(map1.getAllEntriesSnapshot().size());

        map1.put("k1", new SerializedEntry<StringWrapper>(new StringWrapper("v1"), "v1".getBytes(), 0));

        //ClusteredMap<String, byte[]> map2 = master2.getMap("map");

        System.out.println(map1.getAllLocalEntriesSnapshot().size());
        System.out.println(map1.getAllEntriesSnapshot().size());

        System.out.println(map1.get("k1").getDeserializedValue(new SerializationStrategy<StringWrapper>() {

            @Override
            public StringWrapper deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
                return new StringWrapper(new String(bytes));
            }

            @Override
            public StringWrapper deserialize(byte[] bytes, ClassLoader cl) throws IOException, ClassNotFoundException {
                return new StringWrapper(new String(bytes));
            }

            @Override
            public byte[] serialize(StringWrapper t) throws IOException {
                return t.getWrapped().getBytes();
            }

            @Override
            public String generateStringKeyFor(Object o) throws IOException {
                return o.toString();
            }
        }));

        //map1.clear();
    }

    private static class StringWrapper {
        private final String wrapped;

        public StringWrapper(String wrapped) {
            this.wrapped = wrapped;
        }

        public String getWrapped() {
            return wrapped;
        }

        @Override
        public String toString() {
            return wrapped;
        }
    }*/
}
