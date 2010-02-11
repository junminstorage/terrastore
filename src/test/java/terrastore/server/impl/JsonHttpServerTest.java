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
package terrastore.server.impl;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.easymock.classextension.EasyMock;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.junit.Test;
import terrastore.common.ErrorMessage;
import terrastore.server.impl.support.JsonBucketsProvider;
import terrastore.server.impl.support.JsonErrorMessageProvider;
import terrastore.server.impl.support.JsonValuesProvider;
import terrastore.server.impl.support.JsonParametersProvider;
import terrastore.server.impl.support.JsonServerOperationExceptionMapper;
import terrastore.server.impl.support.JsonValueProvider;
import terrastore.service.BackupService;
import terrastore.service.QueryService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import terrastore.store.types.JsonValue;
import terrastore.util.JsonUtils;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class JsonHttpServerTest {

    private static final String JSON_VALUE = "{\"test\":\"test\"}";
    private static final String BAD_JSON_VALUE = "{\"test\":\"test}";
    private static final String JSON_VALUES = "{\"test\":" + JSON_VALUE + "}";
    private static final String JSON_VALUES_x2 = "{\"test1\":" + JSON_VALUE + ",\"test2\":" + JSON_VALUE + "}";
    private static final String UPDATE_PARAMS = "{\"p1\":\"v1\"}";
    private static final String BUCKETS = "[\"test1\",\"test2\"]";

    @Test
    public void testImportBackup() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        backupService.importBackup("bucket", "source", "secret");
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/import?source=source&secret=secret");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testExportBackup() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        backupService.exportBackup("bucket", "destination", "secret");
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/export?destination=destination&secret=secret");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testAddBucket() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.addBucket("bucket");
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testRemoveBucket() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.removeBucket("bucket");
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        DeleteMethod method = new DeleteMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testPutValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.putValue(eq("bucket"), eq("test1"), EasyMock.<Value>anyObject());
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket/test1");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(JSON_VALUE, "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testRemoveValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.removeValue("bucket", "test1");
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        DeleteMethod method = new DeleteMethod("http://localhost:8080/bucket/test1");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testGetValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.getValue("bucket", "test1");
        expectLastCall().andReturn(new JsonValue(JSON_VALUE.getBytes())).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/test1");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUE, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testGetAllValues() throws Exception {
        SortedMap<String, Value> values = new TreeMap<String, Value>();
        values.put("test", new JsonValue(JSON_VALUE.getBytes()));
        BackupService backupService = createMock(BackupService.class);

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);

        queryService.getAllValues(eq("bucket"), eq(0));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testGetBuckets() throws Exception {
        Set<String> buckets = new LinkedHashSet<String>();
        buckets.add("test1");
        buckets.add("test2");

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.getBuckets();
        expectLastCall().andReturn(buckets).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(BUCKETS, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testDoRangeQueryWithNoComparator() throws Exception {
        SortedMap<String, Value> values = new TreeMap<String, Value>();
        values.put("test1", new JsonValue(JSON_VALUE.getBytes()));
        values.put("test2", new JsonValue(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.doRangeQuery(eq("bucket"), eq(new Range("test1", "test2", 0, "")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testDoRangeQueryWithLimit() throws Exception {
        SortedMap<String, Value> values = new TreeMap<String, Value>();
        values.put("test1", new JsonValue(JSON_VALUE.getBytes()));
        values.put("test2", new JsonValue(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.doRangeQuery(eq("bucket"), eq(new Range("test1", "test2", 2, "order")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&limit=2&comparator=order&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testDoRangeQueryWithNoPredicate() throws Exception {
        SortedMap<String, Value> values = new TreeMap<String, Value>();
        values.put("test1", new JsonValue(JSON_VALUE.getBytes()));
        values.put("test2", new JsonValue(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.doRangeQuery(eq("bucket"), eq(new Range("test1", "test2", 0, "order")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&comparator=order&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testDoRangeQueryWithPredicate() throws Exception {
        SortedMap<String, Value> values = new TreeMap<String, Value>();
        values.put("test1", new JsonValue(JSON_VALUE.getBytes()));
        values.put("test2", new JsonValue(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.doRangeQuery(eq("bucket"), eq(new Range("test1", "test2", 0, "order")), eq(new Predicate("test:condition")), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&comparator=order&predicate=test:condition&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testDoPredicateQuery() throws Exception {
        Map<String, Value> values = new HashMap<String, Value>();
        values.put("test1", new JsonValue(JSON_VALUE.getBytes()));
        values.put("test2", new JsonValue(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        queryService.doPredicateQuery(eq("bucket"), eq(new Predicate("test:condition")));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/predicate?predicate=test:condition");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testUpdateValue() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("p1", "v1");

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.executeUpdate(eq("bucket"), eq("key"), eq(new Update("update", 1000, params)));
        expectLastCall().once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/key/update?timeout=1000&function=update");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(UPDATE_PARAMS, "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testJsonErrorMessageOnInternalFail() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        updateService.addBucket("bucket");
        expectLastCall().andThrow(new UpdateOperationException(new ErrorMessage(500, "error"))).once();

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(500, method.getStatusCode());
        assertEquals(toJson(new ErrorMessage(500, "error")), method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    @Test
    public void testJsonErrorMessageOnBadJson() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);

        replay(updateService, queryService, backupService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService);
        TJWSEmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket/key");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(BAD_JSON_VALUE, "application/json", null));
        client.executeMethod(method);

        assertEquals(400, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService);
    }

    private TJWSEmbeddedJaxrsServer startServerWith(JsonHttpServer resource) throws Exception {
        TJWSEmbeddedJaxrsServer server = new TJWSEmbeddedJaxrsServer();
        server.getDeployment().setRegisterBuiltin(true);
        server.getDeployment().setProviderClasses(Arrays.asList(
                JsonErrorMessageProvider.class.getName(),
                JsonValuesProvider.class.getName(),
                JsonBucketsProvider.class.getName(),
                JsonParametersProvider.class.getName(),
                JsonValueProvider.class.getName(),
                JsonServerOperationExceptionMapper.class.getName()));
        server.getDeployment().setResources(Arrays.<Object>asList(resource));
        server.setPort(8080);
        server.start();

        Thread.sleep(1000);

        return server;
    }

    private void stopServer(TJWSEmbeddedJaxrsServer server) throws Exception {
        server.stop();

        Thread.sleep(1000);
    }

    private String toJson(ErrorMessage errorMessage) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(errorMessage, stream);
        return new String(stream.toByteArray());
    }
}
