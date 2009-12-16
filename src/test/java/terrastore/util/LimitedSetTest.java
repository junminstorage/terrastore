package terrastore.util;

import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class LimitedSetTest {

    @Test
    public void testWithLimit() {
        Set<String> source = new TreeSet<String>(com.google.common.collect.Sets.newHashSet("a", "b", "c"));
        Set<String> limited = new LimitedSet<String>(source, 2);
        assertEquals(2, limited.size());
        assertEquals("a", limited.toArray()[0]);
        assertEquals("b", limited.toArray()[1]);
    }

    @Test
    public void testWithNoLimit() {
        Set<String> source = new TreeSet<String>(com.google.common.collect.Sets.newHashSet("a", "b", "c"));
        Set<String> limited = new LimitedSet<String>(source, 0);
        assertEquals(3, limited.size());
        assertEquals("a", limited.toArray()[0]);
        assertEquals("b", limited.toArray()[1]);
        assertEquals("c", limited.toArray()[2]);
    }
}