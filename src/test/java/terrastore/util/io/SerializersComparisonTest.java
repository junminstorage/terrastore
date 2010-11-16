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

import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;
import terrastore.store.Value;

/**
 * @author Sergio Bossa
 */
public class SerializersComparisonTest {
    private static final String VALUE = new String(new byte[1024 * 10]);

    @Test
    public void testJavaSerializer() {
        Value value = new Value(VALUE.getBytes());
        JavaSerializer<Value> serializer = new JavaSerializer();
        StopWatch sw = new StopWatch();
        System.out.println("Warm-up...");
        for (int i = 0; i < 1000; i++) {
            serializer.serialize(value);
        }
        //
        System.out.println("Measuring...");
        sw.start();
        for (int i = 0; i < 100000; i++) {
            serializer.serialize(value);
        }
        sw.stop();
        System.out.println("Elapsed for testJavaSerializer: " + sw.toString());
    }

    @Test
    public void testMsgPackSerializer() {
        Value value = new Value(VALUE.getBytes());
        MsgPackSerializer<Value> serializer = new MsgPackSerializer(false);
        StopWatch sw = new StopWatch();
        System.out.println("Warm-up...");
        for (int i = 0; i < 1000; i++) {
            serializer.serialize(value);
        }
        //
        System.out.println("Measuring...");
        sw.start();
        for (int i = 0; i < 100000; i++) {
            serializer.serialize(value);
        }
        sw.stop();
        System.out.println("Elapsed for testMsgPackSerializer: " + sw.toString());
    }
}
