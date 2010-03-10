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
package terrastore.util.io;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class InputReaderTest {

    private static final String VALUE = new String(new byte[10000]);
    @Test
    public void testRead() throws Exception {
        ByteArrayInputStream stream = new ByteArrayInputStream(VALUE.getBytes());

        InputReader reader = new InputReader();
        byte[] read = reader.read(stream);

        assertArrayEquals(VALUE.getBytes(), read);
    }

    @Test
    public void testMultithreadRead() throws Exception {
        final int total = 100;
        final InputReader reader = new InputReader();
        final List<byte[]> values = new ArrayList<byte[]>(total);

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < total; i++) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        ByteArrayInputStream stream = new ByteArrayInputStream(VALUE.getBytes());
                        byte[] read = reader.read(stream);
                        values.add(read);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.MINUTES);

        for (int i = 0; i < total; i++) {
            assertArrayEquals(VALUE.getBytes(), values.get(i));
        }
    }
}