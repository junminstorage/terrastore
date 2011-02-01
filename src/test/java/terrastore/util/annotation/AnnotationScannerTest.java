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
package terrastore.util.annotation;

import java.util.Map;
import java.util.SortedSet;
import org.junit.Test;
import terrastore.annotation.AutoDetect;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class AnnotationScannerTest {

    @Test
    public void testScan() {
        AnnotationScanner scanner = new AnnotationScanner();
        Map result = scanner.scanByType(AutowiredCommon.class);
        assertEquals(3, result.size());
        assertEquals(AutowiredOne.class, result.get("one").getClass());
        assertEquals(AutowiredTwo.class, result.get("two").getClass());
        assertEquals(AutowiredThree.class, result.get("three").getClass());
    }

    @Test
    public void testOrderedScan() {
        AnnotationScanner scanner = new AnnotationScanner();
        SortedSet result = scanner.orderedScanByType(AutowiredCommon.class);
        assertEquals(3, result.size());
        assertEquals(AutowiredOne.class, result.toArray()[0].getClass());
        assertEquals(AutowiredTwo.class, result.toArray()[1].getClass());
        assertEquals(AutowiredThree.class, result.toArray()[2].getClass());
    }

    @Test
    public void testScanWithNoResults() {
        AnnotationScanner scanner = new AnnotationScanner();
        Map result = scanner.scanByType(Comparable.class);
        assertEquals(0, result.size());
    }

    public static interface AutowiredCommon {}

    @AutoDetect(name = "one", order = 1)
    public static class AutowiredOne implements AutowiredCommon {
    }

    @AutoDetect(name = "two", order = 2)
    public static class AutowiredTwo implements AutowiredCommon {
    }

    @AutoDetect(name = "three", order = 3)
    public static class AutowiredThree implements AutowiredCommon {
    }
}
