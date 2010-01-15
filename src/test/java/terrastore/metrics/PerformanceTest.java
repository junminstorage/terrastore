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
package terrastore.metrics;

import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import terrastore.startup.Startup;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class PerformanceTest {

    private static final String HOST = "127.0.0.1";
    private static final int NODE1_PORT = 8080;
    private static final int NODE2_PORT = 8081;
    private static final int NODE1_SHUTDOWN_PORT = 8280;
    private static final int NODE2_SHUTDOWN_PORT = 8281;
    private static final int SETUP_TIME = 30000;
    private static final int CONCURRENCY = 8;
    private static final HttpClient HTTP_CLIENT = new HttpClient();

    @BeforeClass
    public static void setUpClass() throws Exception {
        HTTP_CLIENT.setHttpConnectionManager(new MultiThreadedHttpConnectionManager());
        System.err.println("Waiting " + SETUP_TIME + " millis for system to set up ...");
        Thread.sleep(SETUP_TIME);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Startup.shutdown(HOST, NODE1_SHUTDOWN_PORT);
        Startup.shutdown(HOST, NODE2_SHUTDOWN_PORT);
    }

    @Test
    public void writeOnly() throws Exception {
        final String bucket = UUID.randomUUID().toString();

        int warmup = 1000;
        int writes = 1000;

        final ExecutorService threadPool = Executors.newFixedThreadPool(CONCURRENCY);
        final CountDownLatch termination = new CountDownLatch(writes);

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        final String payload = getPayload();
        warmUp(warmup, bucket, payload);

        System.err.println("Starting writeOnly performance test.");

        long start = System.currentTimeMillis();
        for (int i = warmup; i < warmup + writes; i++) {
            final int index = i;
            threadPool.execute(new Runnable() {

                public void run() {
                    try {
                        PutMethod putValue = null;
                        putValue = makePutMethod(NODE1_PORT, bucket + "/value" + index);
                        putValue.setRequestEntity(new StringRequestEntity(payload, "application/json", null));
                        HTTP_CLIENT.executeMethod(putValue);
                        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
                        putValue.releaseConnection();
                        termination.countDown();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }

        threadPool.shutdown();
        termination.await(Integer.MAX_VALUE, TimeUnit.SECONDS);

        long elapsed = System.currentTimeMillis() - start;

        System.err.println("Elapsed time in millis: " + elapsed);
    }

    @Test
    public void writeThenRead() throws Exception {
        final String bucket = UUID.randomUUID().toString();
        final String payload = getPayload();

        int warmup = 1000;
        int writes = 1000;

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        warmUp(warmup, bucket, payload);

        System.err.println("Starting writeThenRead performance test.");

        ExecutorService threadPool = Executors.newFixedThreadPool(CONCURRENCY);
        long start = System.currentTimeMillis();
        for (int i = warmup; i < warmup + writes; i++) {
            final int index = i;
            threadPool.execute(new Runnable() {

                public void run() {
                    try {
                        PutMethod putValue = null;
                        putValue = makePutMethod(NODE1_PORT, bucket + "/value" + index);
                        putValue.setRequestEntity(new StringRequestEntity(payload, "application/json", null));
                        HTTP_CLIENT.executeMethod(putValue);
                        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
                        putValue.releaseConnection();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });

        }
        threadPool.shutdown();
        threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        System.err.println("Elapsed write time in millis: " + elapsed);

        threadPool = Executors.newFixedThreadPool(CONCURRENCY);
        start = System.currentTimeMillis();
        for (int i = warmup; i < warmup + writes; i++) {
            final int index = i;

            threadPool.execute(new Runnable() {

                public void run() {
                    try {
                        GetMethod getValue = null;
                        getValue = makeGetMethod(NODE1_PORT, bucket + "/value" + index);
                        HTTP_CLIENT.executeMethod(getValue);
                        assertEquals(HttpStatus.SC_OK, getValue.getStatusCode());
                        getValue.releaseConnection();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }

        threadPool.shutdown();
        threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        elapsed = System.currentTimeMillis() - start;
        System.err.println("Elapsed read time in millis: " + elapsed);
    }

    private String getPayload() throws Exception {
        final String payload = toJson(new TestValue("value", 1));
        System.err.println("Payload bytes length: " + payload.getBytes().length);
        return payload;
    }

    private void warmUp(int warmup, String bucket, String payload) throws RuntimeException {
        System.err.println("Warming up...");
        for (int i = 0; i < warmup; i++) {
            try {
                PutMethod putValue = null;
                putValue = makePutMethod(NODE1_PORT, bucket + "/value" + i);
                putValue.setRequestEntity(new StringRequestEntity(payload, "application/json", null));
                HTTP_CLIENT.executeMethod(putValue);
                assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
                putValue.releaseConnection();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private PutMethod makePutMethod(int nodePort, String path) {
        PutMethod method = new PutMethod("http://" + HOST + ":" + nodePort + "/" + path);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private GetMethod makeGetMethod(int nodePort, String path) {
        GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private String toJson(TestValue value) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        StringWriter result = new StringWriter();
        jsonMapper.writeValue(result, value);
        return result.toString();
    }

    private static class TestValue {

        private String stringField;
        private int numField;
        private String[] constantArray = new String[]{"a", "b", "c"};
        private InnerValue constantValue = new InnerValue("inner", 1);

        public TestValue(String stringField, int numField) {
            this.stringField = stringField;
            this.numField = numField;
        }

        protected TestValue() {
        }

        public String getStringField() {
            return stringField;
        }

        public int getNumField() {
            return numField;
        }

        public String[] getConstantArray() {
            return constantArray;
        }

        public InnerValue getConstantValue() {
            return constantValue;
        }

        private void setStringField(String stringField) {
            this.stringField = stringField;
        }

        private void setNumField(int numField) {
            this.numField = numField;
        }

        private void setConstantArray(String[] constantArray) {
            this.constantArray = constantArray;
        }

        private void setConstantValue(InnerValue constantValue) {
            this.constantValue = constantValue;
        }

        @Override
        public boolean equals(Object obj) {
            TestValue other = (TestValue) obj;
            return this.stringField.equals(other.stringField) && this.numField == other.numField;
        }

        @Override
        public int hashCode() {
            return stringField.hashCode() * numField;
        }

        private static class InnerValue {

            private String stringField;
            private int numField;
            private String[] constantArray = new String[]{"a", "b", "c"};

            public InnerValue(String stringField, int numField) {
                this.stringField = stringField;
                this.numField = numField;
            }

            protected InnerValue() {
            }

            public String getStringField() {
                return stringField;
            }

            public int getNumField() {
                return numField;
            }

            public String[] getConstantArray() {
                return constantArray;
            }

            private void setStringField(String stringField) {
                this.stringField = stringField;
            }

            private void setNumField(int numField) {
                this.numField = numField;
            }

            private void setConstantArray(String[] constantArray) {
                this.constantArray = constantArray;
            }

            @Override
            public boolean equals(Object obj) {
                InnerValue other = (InnerValue) obj;
                return this.stringField.equals(other.stringField) && this.numField == other.numField;
            }

            @Override
            public int hashCode() {
                return stringField.hashCode() * numField;
            }
        }
    }
}
