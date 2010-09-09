package terrastore.util.collect;

import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class TransformedSetTest {

    @Test
    public void testTransformation() {
        Set<Integer> source = new LinkedHashSet<Integer>();
        for (int i = 0; i < 10; i++) {
            source.add(i);
        }

        Set<String> transformed = new TransformedSet<Integer, String>(source, new Transformer<Integer, String>() {

            @Override
            public String transform(Integer input) {
                return input.toString();
            }
        });

        int i = 0;
        for (String t : transformed) {
            assertEquals("" + i++, t);
        }
    }
}