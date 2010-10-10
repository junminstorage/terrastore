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
import java.util.LinkedHashMap;
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
import org.jboss.resteasy.plugins.server.embedded.EmbeddedJaxrsServer;
import org.junit.Test;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.impl.support.JsonBucketsProvider;
import terrastore.server.impl.support.JsonClusterStatsProvider;
import terrastore.server.impl.support.JsonErrorMessageProvider;
import terrastore.server.impl.support.JsonValuesProvider;
import terrastore.server.impl.support.JsonParametersProvider;
import terrastore.server.impl.support.JsonServerOperationExceptionMapper;
import terrastore.server.impl.support.JsonValueProvider;
import terrastore.service.BackupService;
import terrastore.service.QueryService;
import terrastore.service.StatsService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import terrastore.store.Value;
import terrastore.test.support.JettyJaxrsServer;
import terrastore.util.collect.Sets;
import terrastore.util.json.JsonUtils;
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
    private static final String CLUSTER_STATS = "{\"clusters\":[{\"name\":\"cluster-1\",\"status\":\"AVAILABLE\",\"nodes\":[{\"name\":\"node-1\",\"host\":\"localhost\",\"port\":8080}]}]}";

    @Test
    public void testGetStats() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        ClusterStats.Cluster cluster = new ClusterStats.Cluster("cluster-1", Sets.linked(new ClusterStats.Node("node-1", "localhost", 8080)));
        ClusterStats clusterStats = new ClusterStats(Sets.linked(cluster));

        statsService.getClusterStats();
        expectLastCall().andReturn(clusterStats).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/_stats/cluster");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(CLUSTER_STATS, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testImportBackup() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        backupService.importBackup("bucket", "source", "secret");
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/import?source=source&secret=secret");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testExportBackup() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        backupService.exportBackup("bucket", "destination", "secret");
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/export?destination=destination&secret=secret");
        method.setRequestEntity(new StringRequestEntity("", "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testRemoveBucket() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.removeBucket("bucket");
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        DeleteMethod method = new DeleteMethod("http://localhost:8080/bucket");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testPutValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.putValue(eq("bucket"), eq(new Key("test1")), EasyMock.<Value>anyObject(), eq(new Predicate(null)));
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket/test1");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(JSON_VALUE, "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testPutValueWithPredicate() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.putValue(eq("bucket"), eq(new Key("test1")), EasyMock.<Value>anyObject(), eq(new Predicate("test:condition")));
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket/test1?predicate=test:condition");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(JSON_VALUE, "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testRemoveValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.removeValue("bucket", new Key("test1"));
        expectLastCall().once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        DeleteMethod method = new DeleteMethod("http://localhost:8080/bucket/test1");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_NO_CONTENT, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testGetValue() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.getValue(eq("bucket"), eq(new Key("test1")), eq(new Predicate(null)));
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/test1");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUE, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testGetValueWithPredicate() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.getValue(eq("bucket"), eq(new Key("test1")), eq(new Predicate("test:condition")));
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/test1?predicate=test:condition");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUE, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testGetAllValues() throws Exception {
        SortedMap<Key, Value> values = new TreeMap<Key, Value>();
        values.put(new Key("test"), new Value(JSON_VALUE.getBytes()));
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);

        queryService.getAllValues(eq("bucket"), eq(0));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testGetBuckets() throws Exception {
        Set<String> buckets = new LinkedHashSet<String>();
        buckets.add("test1");
        buckets.add("test2");

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.getBuckets();
        expectLastCall().andReturn(buckets).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(BUCKETS, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testQueryByRangeWithNoComparator() throws Exception {
        SortedMap<Key, Value> values = new TreeMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.queryByRange(eq("bucket"), eq(new Range(new Key("test1"), new Key("test2"), 0, "")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testQueryByRangeWithLimit() throws Exception {
        SortedMap<Key, Value> values = new TreeMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.queryByRange(eq("bucket"), eq(new Range(new Key("test1"), new Key("test2"), 2, "order")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&limit=2&comparator=order&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testQueryByRangeWithNoPredicate() throws Exception {
        SortedMap<Key, Value> values = new TreeMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.queryByRange(eq("bucket"), eq(new Range(new Key("test1"), new Key("test2"), 0, "order")), eq(new Predicate(null)), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&comparator=order&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testQueryByRangeWithPredicate() throws Exception {
        SortedMap<Key, Value> values = new TreeMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.queryByRange(eq("bucket"), eq(new Range(new Key("test1"), new Key("test2"), 0, "order")), eq(new Predicate("test:condition")), eq(0L));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/range?startKey=test1&endKey=test2&comparator=order&predicate=test:condition&timeToLive=0");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testQueryByPredicate() throws Exception {
        Map<Key, Value> values = new LinkedHashMap<Key, Value>();
        values.put(new Key("test1"), new Value(JSON_VALUE.getBytes()));
        values.put(new Key("test2"), new Value(JSON_VALUE.getBytes()));

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        queryService.queryByPredicate(eq("bucket"), eq(new Predicate("test:condition")));
        expectLastCall().andReturn(values).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod("http://localhost:8080/bucket/predicate?predicate=test:condition");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        System.err.println(method.getResponseBodyAsString());
        assertEquals(JSON_VALUES_x2, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testUpdateValue() throws Exception {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("p1", "v1");

        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.updateValue(eq("bucket"), eq(new Key("key")), eq(new Update("update", 1000, params)));
        expectLastCall().andReturn(new Value(JSON_VALUE.getBytes())).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PostMethod method = new PostMethod("http://localhost:8080/bucket/key/update?timeout=1000&function=update");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(UPDATE_PARAMS, "application/json", null));
        client.executeMethod(method);

        assertEquals(HttpStatus.SC_OK, method.getStatusCode());
        assertEquals(JSON_VALUE, method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testJsonErrorMessageOnInternalFail() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        updateService.removeBucket("bucket");
        expectLastCall().andThrow(new UpdateOperationException(new ErrorMessage(500, "error"))).once();

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        DeleteMethod method = new DeleteMethod("http://localhost:8080/bucket");
        method.setRequestHeader("Content-Type", "application/json");
        client.executeMethod(method);

        assertEquals(500, method.getStatusCode());
        assertEquals(toJson(new ErrorMessage(500, "error")), method.getResponseBodyAsString());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    @Test
    public void testJsonErrorMessageOnBadJson() throws Exception {
        UpdateService updateService = createMock(UpdateService.class);
        QueryService queryService = createMock(QueryService.class);
        BackupService backupService = createMock(BackupService.class);
        StatsService statsService = createMock(StatsService.class);

        replay(updateService, queryService, backupService, statsService);

        JsonHttpServer serverResource = new JsonHttpServer(updateService, queryService, backupService, statsService);
        EmbeddedJaxrsServer server = startServerWith(serverResource);
        HttpClient client = new HttpClient();
        PutMethod method = new PutMethod("http://localhost:8080/bucket/key");
        method.setRequestHeader("Content-Type", "application/json");
        method.setRequestEntity(new StringRequestEntity(BAD_JSON_VALUE, "application/json", null));
        client.executeMethod(method);

        assertEquals(400, method.getStatusCode());

        method.releaseConnection();

        stopServer(server);

        verify(updateService, queryService, backupService, statsService);
    }

    private EmbeddedJaxrsServer startServerWith(JsonHttpServer resource) throws Exception {
        JettyJaxrsServer server = new JettyJaxrsServer("127.0.0.1", 8080);
        server.getDeployment().setRegisterBuiltin(true);
        server.getDeployment().setProviderClasses(Arrays.asList(
                JsonErrorMessageProvider.class.getName(),
                JsonValuesProvider.class.getName(),
                JsonBucketsProvider.class.getName(),
                JsonParametersProvider.class.getName(),
                JsonValueProvider.class.getName(),
                JsonServerOperationExceptionMapper.class.getName(),
                JsonClusterStatsProvider.class.getName()));
        server.getDeployment().setResources(Arrays.<Object>asList(resource));
        server.start();

        Thread.sleep(1000);

        return server;
    }

    private void stopServer(EmbeddedJaxrsServer server) throws Exception {
        server.stop();

        Thread.sleep(1000);
    }

    private String toJson(ErrorMessage errorMessage) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonUtils.write(errorMessage, stream);
        return new String(stream.toByteArray());
    }
}
