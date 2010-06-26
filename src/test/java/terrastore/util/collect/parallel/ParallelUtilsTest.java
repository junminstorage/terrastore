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
package terrastore.util.collect.parallel;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import terrastore.util.collect.Sets;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ParallelUtilsTest {

    @Test
    public void testParallelMerge() {
        Set<String> merged = ParallelUtils.parallelMerge(Lists.newArrayList(
                Sets.linked("6", "7", "8"),
                Sets.linked("11", "12"),
                Sets.linked("1", "2", "3"),
                Sets.linked("9", "10"),
                Sets.linked("4", "5")));
        assertEquals(Sets.linked("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"), merged);
    }

    @Test
    public void testParallelMap() {
        List<String> result = ParallelUtils.parallelMap(
                Lists.newArrayList("David Gilmour", "Jimmy Page", "Carlos Santana"),
                new MapCollector<String, List<String>>() {

                    @Override
                    public List<String> createCollector() {
                        return new LinkedList<String>();
                    }
                },
                new MapTask<String, String, List<String>>() {

                    @Override
                    public List<String> map(String input, MapCollector<String, List<String>> collector) {
                        List<String> result = collector.createCollector();
                        try {
                            String[] tokens = input.split(" ");
                            result.addAll(Arrays.asList(tokens));
                            return result;
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            return result;
                        }
                    }
                });
        assertEquals(6, result.size());
        assertTrue(result.contains("David"));
        assertTrue(result.contains("Gilmour"));
        assertTrue(result.contains("Jimmy"));
        assertTrue(result.contains("Page"));
        assertTrue(result.contains("Carlos"));
        assertTrue(result.contains("Santana"));
    }
}
