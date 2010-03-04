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
package terrastore.integration;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
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
public class IntegrationTest {

    private static final String HOST = "127.0.0.1";
    private static final int NODE1_PORT = 8080;
    private static final int NODE2_PORT = 8081;
    private static final int NODE1_SHUTDOWN_PORT = 8280;
    private static final int NODE2_SHUTDOWN_PORT = 8281;
    private static final int SETUP_TIME = 30000;
    private HttpClient HTTP_CLIENT = new HttpClient();

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.err.println("Waiting " + SETUP_TIME + " millis for system to set up ...");
        Thread.sleep(SETUP_TIME);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Startup.shutdown(HOST, NODE1_SHUTDOWN_PORT);
        Startup.shutdown(HOST, NODE2_SHUTDOWN_PORT);
    }

    @Test
    public void testCreateBucketsAndGetNamesOnOtherNode() throws Exception {
        String bucket1 = UUID.randomUUID().toString();
        String bucket2 = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket1);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();
        addBucket = makePutMethod(NODE1_PORT, bucket2);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        GetMethod getBuckets = makeGetMethod(NODE2_PORT, "");
        HTTP_CLIENT.executeMethod(getBuckets);
        assertEquals(HttpStatus.SC_OK, getBuckets.getStatusCode());
        System.err.println(getBuckets.getResponseBodyAsString());
        getBuckets.releaseConnection();
    }

    @Test
    public void testCreateBucketPutValueAndGetOnOtherNode() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        TestValue value = new TestValue("value", 1);
        PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value");
        putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
        HTTP_CLIENT.executeMethod(putValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
        putValue.releaseConnection();

        GetMethod getValue = makeGetMethod(NODE2_PORT, bucket + "/value");
        HTTP_CLIENT.executeMethod(getValue);
        assertEquals(HttpStatus.SC_OK, getValue.getStatusCode());
        TestValue returned = fromJsonToObject(getValue.getResponseBodyAsString());
        assertEquals(value, returned);
        getValue.releaseConnection();
    }

    @Test
    public void testCreateBucketPutValueAndConditionallyPutAgainWithSuccess() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        TestValue value = new TestValue("value1", 1);
        PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value");
        putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
        HTTP_CLIENT.executeMethod(putValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
        putValue.releaseConnection();

        TestValue newValue = new TestValue("value2", 1);
        PutMethod conditionallyPutValue = makePutMethodWithPredicate(NODE1_PORT, bucket + "/value", "jxpath:/stringField[.='value1']");
        conditionallyPutValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(newValue), "application/json", null));
        HTTP_CLIENT.executeMethod(conditionallyPutValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, conditionallyPutValue.getStatusCode());
        conditionallyPutValue.releaseConnection();

        GetMethod getValue = makeGetMethod(NODE2_PORT, bucket + "/value");
        HTTP_CLIENT.executeMethod(getValue);
        assertEquals(HttpStatus.SC_OK, getValue.getStatusCode());
        TestValue returned = fromJsonToObject(getValue.getResponseBodyAsString());
        assertEquals(newValue, returned);
        getValue.releaseConnection();
    }

    @Test
    public void testCreateBucketPutValueAndConditionallyPutAgainWithConflict() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        TestValue value = new TestValue("value1", 1);
        PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value");
        putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
        HTTP_CLIENT.executeMethod(putValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
        putValue.releaseConnection();

        TestValue newValue = new TestValue("value2", 1);
        PutMethod conditionallyPutValue = makePutMethodWithPredicate(NODE1_PORT, bucket + "/value", "jxpath:/stringField[.='wrong']");
        conditionallyPutValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(newValue), "application/json", null));
        HTTP_CLIENT.executeMethod(conditionallyPutValue);
        assertEquals(HttpStatus.SC_CONFLICT, conditionallyPutValue.getStatusCode());
        conditionallyPutValue.releaseConnection();

        GetMethod getValue = makeGetMethod(NODE2_PORT, bucket + "/value");
        HTTP_CLIENT.executeMethod(getValue);
        assertEquals(HttpStatus.SC_OK, getValue.getStatusCode());
        TestValue returned = fromJsonToObject(getValue.getResponseBodyAsString());
        assertEquals(value, returned);
        getValue.releaseConnection();
    }

    @Test
    public void testCreateBucketPutValueAndDeleteBothOnOtherNode() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        TestValue value = new TestValue("value", 1);
        PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value");
        putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
        HTTP_CLIENT.executeMethod(putValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
        putValue.releaseConnection();

        DeleteMethod deleteValue = makeDeleteMethod(NODE2_PORT, bucket + "/value");
        HTTP_CLIENT.executeMethod(deleteValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, deleteValue.getStatusCode());
        deleteValue.releaseConnection();

        DeleteMethod deleteBucket = makeDeleteMethod(NODE2_PORT, bucket);
        HTTP_CLIENT.executeMethod(deleteBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, deleteBucket.getStatusCode());
        deleteBucket.releaseConnection();
    }

    @Test
    public void testGetAllValuesWithNoLimit() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod getAllValues = makeGetMethod(NODE2_PORT, bucket);
        HTTP_CLIENT.executeMethod(getAllValues);
        assertEquals(HttpStatus.SC_OK, getAllValues.getStatusCode());
        Map<String, Object> allValues = fromJsonToMap(getAllValues.getResponseBodyAsString());
        System.err.println(getAllValues.getResponseBodyAsString());
        getAllValues.releaseConnection();
        assertEquals(size, allValues.size());
        for (int i = 1; i <= size; i++) {
            assertTrue(allValues.containsKey("value" + (char) ('a' + i)));
        }
    }

    @Test
    public void testGetAllValuesWithLimit() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        int limit = 5;

        GetMethod getAllValues = makeGetMethodWithLimit(NODE2_PORT, bucket, limit);
        HTTP_CLIENT.executeMethod(getAllValues);
        assertEquals(HttpStatus.SC_OK, getAllValues.getStatusCode());
        Map<String, Object> allValues = fromJsonToMap(getAllValues.getResponseBodyAsString());
        System.err.println(getAllValues.getResponseBodyAsString());
        getAllValues.releaseConnection();
        assertEquals(limit, allValues.size());
    }

    @Test
    public void testRangeQueryWithDefaultComparator() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "valueb", "valued");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(3, values.size());
        assertEquals("valueb", values.keySet().toArray()[0]);
        assertEquals("valuec", values.keySet().toArray()[1]);
        assertEquals("valued", values.keySet().toArray()[2]);
    }

    @Test
    public void testRangeQueryWithStringKeys() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "valueb", "valued", "lexical-asc");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(3, values.size());
        assertEquals("valueb", values.keySet().toArray()[0]);
        assertEquals("valuec", values.keySet().toArray()[1]);
        assertEquals("valued", values.keySet().toArray()[2]);
    }

    @Test
    public void testRangeQueryWithNumberKeys() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/" + i);
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "1", "10", "numeric-asc");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(10, values.size());
        assertEquals("1", values.keySet().toArray()[0]);
        assertEquals("10", values.keySet().toArray()[9]);
    }

    @Test
    public void testRangeQueryWithLimit() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "valueb", "valued", 2, "lexical-asc");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(2, values.size());
        assertEquals("valueb", values.keySet().toArray()[0]);
        assertEquals("valuec", values.keySet().toArray()[1]);
    }

    @Test
    public void testRangeQueryWithNoEndKeyAndNoLimit() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "valuej", 0, "lexical-asc");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(2, values.size());
        assertEquals("valuej", values.keySet().toArray()[0]);
        assertEquals("valuek", values.keySet().toArray()[1]);
    }

    @Test
    public void testRangeQueryWithPredicate() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithRange(NODE2_PORT, bucket + "/range", "valueb", "valued", "lexical-asc", "jxpath:/stringField[.='value1']");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(1, values.size());
        assertEquals("valueb", values.keySet().toArray()[0]);
    }

    @Test
    public void testPredicateQuery() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        GetMethod doRangeQuery = makeGetMethodWithPredicate(NODE2_PORT, bucket + "/predicate", "jxpath:/stringField[.='value2']");
        HTTP_CLIENT.executeMethod(doRangeQuery);
        assertEquals(HttpStatus.SC_OK, doRangeQuery.getStatusCode());
        Map<String, Object> values = fromJsonToMap(doRangeQuery.getResponseBodyAsString());
        System.err.println(doRangeQuery.getResponseBodyAsString());
        doRangeQuery.releaseConnection();
        assertEquals(1, values.size());
        assertEquals("valuec", values.keySet().toArray()[0]);
    }

    @Test
    public void testUpdateValue() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        TestValue value = new TestValue("value", 1);
        PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value");
        putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
        HTTP_CLIENT.executeMethod(putValue);
        assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
        putValue.releaseConnection();

        TestValue params = new TestValue("value2", 2);
        PostMethod postUpdate = makePostMethodForUpdate(NODE2_PORT, bucket + "/value/update", 1000, "replace");
        postUpdate.setRequestEntity(new StringRequestEntity(fromObjectToJson(params), "application/json", null));
        HTTP_CLIENT.executeMethod(postUpdate);
        assertEquals(HttpStatus.SC_NO_CONTENT, postUpdate.getStatusCode());
        postUpdate.releaseConnection();

        GetMethod getValue = makeGetMethod(NODE2_PORT, bucket + "/value");
        HTTP_CLIENT.executeMethod(getValue);
        assertEquals(HttpStatus.SC_OK, getValue.getStatusCode());
        TestValue returned = fromJsonToObject(getValue.getResponseBodyAsString());
        assertEquals("value2", returned.getStringField());
        assertEquals(2, returned.getNumField());
        getValue.releaseConnection();
    }

    @Test
    public void testBackup() throws Exception {
        String bucket = UUID.randomUUID().toString();

        PutMethod addBucket = makePutMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(addBucket);
        assertEquals(HttpStatus.SC_NO_CONTENT, addBucket.getStatusCode());
        addBucket.releaseConnection();

        int size = 10;

        for (int i = 1; i <= size; i++) {
            TestValue value = new TestValue("value" + i, i);
            PutMethod putValue = makePutMethod(NODE1_PORT, bucket + "/value" + (char) ('a' + i));
            putValue.setRequestEntity(new StringRequestEntity(fromObjectToJson(value), "application/json", null));
            HTTP_CLIENT.executeMethod(putValue);
            assertEquals(HttpStatus.SC_NO_CONTENT, putValue.getStatusCode());
            putValue.releaseConnection();
        }

        PostMethod postUpdate = makePostMethodForBackupExport(NODE1_PORT, bucket + "/export", "test.bak", "SECRET-KEY");
        postUpdate.setRequestEntity(new StringRequestEntity("", "application/json", null));
        HTTP_CLIENT.executeMethod(postUpdate);
        assertEquals(HttpStatus.SC_NO_CONTENT, postUpdate.getStatusCode());
        postUpdate.releaseConnection();

        postUpdate = makePostMethodForBackupImport(NODE1_PORT, bucket + "/import", "test.bak", "SECRET-KEY");
        postUpdate.setRequestEntity(new StringRequestEntity("", "application/json", null));
        HTTP_CLIENT.executeMethod(postUpdate);
        assertEquals(HttpStatus.SC_NO_CONTENT, postUpdate.getStatusCode());
        postUpdate.releaseConnection();

        GetMethod getAllValues = makeGetMethod(NODE1_PORT, bucket);
        HTTP_CLIENT.executeMethod(getAllValues);
        assertEquals(HttpStatus.SC_OK, getAllValues.getStatusCode());
        Map<String, Object> allValues = fromJsonToMap(getAllValues.getResponseBodyAsString());
        System.err.println(getAllValues.getResponseBodyAsString());
        getAllValues.releaseConnection();
        assertEquals(size, allValues.size());
        for (int i = 1; i <= size; i++) {
            assertTrue(allValues.containsKey("value" + (char) ('a' + i)));
        }
    }

    private PutMethod makePutMethod(int nodePort, String path) {
        PutMethod method = new PutMethod("http://" + HOST + ":" + nodePort + "/" + path);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private PutMethod makePutMethodWithPredicate(int nodePort, String path, String predicate) {
        try {
            PutMethod method = new PutMethod("http://" + HOST + ":" + nodePort + "/" + path + "?predicate=" + URLEncoder.encode(predicate, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private DeleteMethod makeDeleteMethod(int nodePort, String path) {
        DeleteMethod method = new DeleteMethod("http://" + HOST + ":" + nodePort + "/" + path);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private GetMethod makeGetMethod(int nodePort, String path) {
        GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private GetMethod makeGetMethodWithLimit(int nodePort, String path, int limit) {
        GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?limit=" + limit);
        method.setRequestHeader("Content-Type", "application/json");
        return method;
    }

    private GetMethod makeGetMethodWithRange(int nodePort, String path, String startKey, String endKey) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?startKey=" + URLEncoder.encode(startKey, "UTF-8") + "&endKey=" + URLEncoder.
                    encode(endKey, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private GetMethod makeGetMethodWithRange(int nodePort, String path, String startKey, String endKey, String comparator) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?startKey=" + URLEncoder.encode(startKey, "UTF-8") + "&endKey=" + URLEncoder.
                    encode(endKey, "UTF-8") + "&comparator=" + URLEncoder.encode(comparator, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private GetMethod makeGetMethodWithRange(int nodePort, String path, String startKey, String endKey, int limit, String comparator) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?startKey=" + URLEncoder.encode(startKey, "UTF-8") + "&endKey=" + URLEncoder.
                    encode(endKey, "UTF-8") + "&comparator=" + URLEncoder.encode(comparator, "UTF-8") + "&limit=" + URLEncoder.encode(new Integer(limit).
                    toString(), "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private GetMethod makeGetMethodWithRange(int nodePort, String path, String startKey, int limit, String comparator) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?startKey=" + URLEncoder.encode(startKey, "UTF-8") + "&comparator=" + URLEncoder.
                    encode(comparator, "UTF-8") + "&limit=" + URLEncoder.encode(new Integer(limit).toString(), "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private GetMethod makeGetMethodWithRange(int nodePort, String path, String startKey, String endKey, String comparator, String predicate) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?startKey=" + URLEncoder.encode(startKey, "UTF-8") + "&endKey=" + URLEncoder.
                    encode(endKey, "UTF-8") + "&comparator=" + URLEncoder.encode(comparator, "UTF-8") + "&predicate=" + URLEncoder.encode(predicate, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private GetMethod makeGetMethodWithPredicate(int nodePort, String path, String predicate) {
        try {
            GetMethod method = new GetMethod("http://" + HOST + ":" + nodePort + "/" + path + "?predicate=" + URLEncoder.encode(predicate, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private PostMethod makePostMethodForUpdate(int nodePort, String path, long timeout, String function) {
        try {
            PostMethod method = new PostMethod("http://" + HOST + ":" + nodePort + "/" + path + "?timeout=" + timeout + "&function=" + URLEncoder.encode(function, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private PostMethod makePostMethodForBackupExport(int nodePort, String path, String destination, String secret) {
        try {
            PostMethod method = new PostMethod("http://" + HOST + ":" + nodePort + "/" + path + "?destination=" + destination + "&secret=" + URLEncoder.encode(secret, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private PostMethod makePostMethodForBackupImport(int nodePort, String path, String source, String secret) {
        try {
            PostMethod method = new PostMethod("http://" + HOST + ":" + nodePort + "/" + path + "?source=" + source + "&secret=" + URLEncoder.encode(secret, "UTF-8"));
            method.setRequestHeader("Content-Type", "application/json");
            return method;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("Unsupported UTF-8 encoding.");
        }
    }

    private String fromObjectToJson(TestValue value) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        StringWriter result = new StringWriter();
        jsonMapper.writeValue(result, value);
        return result.toString();
    }

    private TestValue fromJsonToObject(String content) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.<TestValue>readValue(content, TestValue.class);
    }

    private Map<String, Object> fromJsonToMap(String content) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.<Map>readValue(content, Map.class);
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
