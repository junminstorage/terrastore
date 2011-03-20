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
package terrastore.server.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.plugins.providers.StringTextStar;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Keys;
import terrastore.server.MapReduceDescriptor;
import terrastore.server.Parameters;
import terrastore.server.Server;
import terrastore.server.ServerOperationException;
import terrastore.server.Values;
import terrastore.server.impl.cors.CorsController;
import terrastore.server.impl.cors.CorsInterceptor;
import terrastore.server.impl.support.JsonBucketsProvider;
import terrastore.server.impl.support.JsonClusterStatsProvider;
import terrastore.server.impl.support.JsonErrorMessageProvider;
import terrastore.server.impl.support.JsonKeysProvider;
import terrastore.server.impl.support.JsonMapReduceDescriptorProvider;
import terrastore.server.impl.support.JsonParametersProvider;
import terrastore.server.impl.support.JsonServerOperationExceptionMapper;
import terrastore.server.impl.support.JsonValueProvider;
import terrastore.server.impl.support.JsonValuesProvider;
import terrastore.store.Key;
import terrastore.store.Value;

/**
 * Jetty-based JAX-RS json-over-http server.
 *
 * @author Sergio Bossa
 */
@Path("/")
public class JsonHttpServer {

    public final static String HTTP_THREADS_CONFIGURATION_PARAMETER = "configuration.httpThreads";
    public final static String CORS_ALLOWED_ORIGINS_CONFIGURATION_PARAMETER = "configuration.corsAllowedOrigins";
    //
    private static final Logger LOG = LoggerFactory.getLogger(JsonHttpServer.class);
    //
    private final Server core;
    private org.mortbay.jetty.Server jetty;

    public JsonHttpServer(Server coreServer) {
        this.core = coreServer;
    }

    public synchronized void start(String host, int port, Map<String, String> configuration) throws Exception {
        ResteasyDeployment deployment = new ResteasyDeployment();
        registerProviders(deployment, configuration);
        registerResources(deployment, configuration);
        startServer(host, port, deployment, configuration);
    }

    public synchronized void stop() throws Exception {
        jetty.stop();
    }

    @DELETE
    @Path("/{bucket}")
    public Response removeBucket(@PathParam("bucket") String bucket) throws ServerOperationException {
        core.removeBucket(bucket);
        return Response.noContent().build();
    }

    @POST
    @Path("/{bucket}/bulk/put")
    @Consumes("application/json")
    @Produces("application/json")
    public Response bulkPut(@PathParam("bucket") String bucket, Values values) throws ServerOperationException {
        Keys insertedKeys = core.bulkPut(bucket, values);
        return Response.ok().entity(insertedKeys).build();
    }

    @POST
    @Path("/{bucket}/bulk/get")
    @Consumes("application/json")
    @Produces("application/json")
    public Response bulkGet(@PathParam("bucket") String bucket, Keys keys) throws ServerOperationException {
        Values fetchedValues = core.bulkGet(bucket, keys);
        return Response.ok().entity(fetchedValues).build();
    }

    @PUT
    @Path("/{bucket}/{key}")
    @Consumes("application/json")
    public Response putValue(@PathParam("bucket") String bucket, @PathParam("key") Key key, Value value, @QueryParam("predicate") String predicate) throws ServerOperationException {
        core.putValue(bucket, key, value, predicate);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/{bucket}/{key}")
    public Response removeValue(@PathParam("bucket") String bucket, @PathParam("key") Key key) throws ServerOperationException {
        core.removeValue(bucket, key);
        return Response.noContent().build();
    }

    @POST
    @Path("/{bucket}/{key}/update")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updateValue(@PathParam("bucket") String bucket, @PathParam("key") Key key, @QueryParam("function") String function, @QueryParam("timeout") Long timeout, Parameters parameters) throws ServerOperationException {
        try {
            Value result = core.updateValue(bucket, key, function, timeout, parameters);
            return Response.ok(result).contentLocation(new URI(bucket + "/" + key)).build();
        } catch (URISyntaxException ex) {
            throw new ServerOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

    @POST
    @Path("/{bucket}/{key}/merge")
    @Consumes("application/json")
    @Produces("application/json")
    public Response mergeValue(@PathParam("bucket") String bucket, @PathParam("key") Key key, Value value) throws ServerOperationException {
        try {
            Value result = core.mergeValue(bucket, key, value);
            return Response.ok(result).contentLocation(new URI(bucket + "/" + key)).build();
        } catch (URISyntaxException ex) {
            throw new ServerOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, ex.getMessage()));
        }
    }

    @GET
    @Path("/")
    @Produces("application/json")
    public Response getBuckets() throws ServerOperationException {
        Buckets result = core.getBuckets();
        return Response.ok(result).build();
    }

    @GET
    @Path("/{bucket}/{key}")
    @Produces("application/json")
    public Response getValue(@PathParam("bucket") String bucket, @PathParam("key") Key key, @QueryParam("predicate") String predicate) throws ServerOperationException {
        Value result = core.getValue(bucket, key, predicate);
        return Response.ok(result).build();
    }

    @GET
    @Path("/{bucket}")
    @Produces("application/json")
    public Response getAllValues(@PathParam("bucket") String bucket, @QueryParam("limit") int limit) throws ServerOperationException {
        Values result = core.getAllValues(bucket, limit);
        return Response.ok(result).build();
    }

    @GET
    @Path("/{bucket}/range")
    @Produces("application/json")
    public Response queryByRange(@PathParam("bucket") String bucket, @QueryParam("startKey") Key startKey, @QueryParam("endKey") Key endKey, @QueryParam("limit") int limit, @QueryParam("comparator") String comparator, @QueryParam("predicate") String predicateExpression, @QueryParam("timeToLive") long timeToLive) throws ServerOperationException {
        Values result = core.queryByRange(bucket, startKey, endKey, limit, comparator, predicateExpression, timeToLive);
        return Response.ok(result).build();
    }

    @DELETE
    @Path("/{bucket}/range")
    @Produces("application/json")
    public Response removeByRange(@PathParam("bucket") String bucket, @QueryParam("startKey") Key startKey, @QueryParam("endKey") Key endKey, @QueryParam("limit") int limit, @QueryParam("comparator") String comparator, @QueryParam("predicate") String predicateExpression, @QueryParam("timeToLive") long timeToLive) throws ServerOperationException {
        Keys removedKeys = core.removeByRange(bucket, startKey, endKey, limit, comparator, predicateExpression, timeToLive);
        return Response.ok(removedKeys).build();
    }

    @GET
    @Path("/{bucket}/predicate")
    @Produces("application/json")
    public Response queryByPredicate(@PathParam("bucket") String bucket, @QueryParam("predicate") String predicateExpression) throws ServerOperationException {
        Values result = core.queryByPredicate(bucket, predicateExpression);
        return Response.ok(result).build();
    }

    @POST
    @Path("/{bucket}/mapReduce")
    @Consumes("application/json")
    @Produces("application/json")
    public Response queryByMapReduce(@PathParam("bucket") String bucket, MapReduceDescriptor descriptor) throws ServerOperationException {
        Value result = core.queryByMapReduce(bucket, descriptor);
        return Response.ok(result).build();
    }

    @POST
    @Path("/{bucket}/import")
    public Response importBackup(@PathParam("bucket") String bucket, @QueryParam("source") String source, @QueryParam("secret") String secret) throws ServerOperationException {
        core.importBackup(bucket, source, secret);
        return Response.noContent().build();
    }

    @POST
    @Path("/{bucket}/export")
    public Response exportBackup(@PathParam("bucket") String bucket, @QueryParam("destination") String destination, @QueryParam("secret") String secret) throws ServerOperationException {
        core.exportBackup(bucket, destination, secret);
        return Response.noContent().build();
    }

    @GET
    @Path("/_stats/cluster")
    @Produces("application/json")
    public Response getClusterStats() {
        ClusterStats result = core.getClusterStats();
        return Response.ok(result).build();
    }

    private void registerProviders(ResteasyDeployment deployment, Map<String, String> configuration) {
        List providers = Arrays.asList(
                new JsonKeysProvider(),
                new JsonBucketsProvider(),
                new JsonValuesProvider(),
                new JsonValueProvider(),
                new JsonClusterStatsProvider(),
                new JsonErrorMessageProvider(),
                new JsonParametersProvider(),
                new JsonMapReduceDescriptorProvider(),
                new JsonServerOperationExceptionMapper(),
                new StringTextStar(),
                new CorsInterceptor(
                configuration.get(CORS_ALLOWED_ORIGINS_CONFIGURATION_PARAMETER),
                "POST, GET, PUT, DELETE, OPTIONS",
                "CONTENT-TYPE",
                "86400"));
        deployment.setProviders(providers);
    }

    private void registerResources(ResteasyDeployment deployment, Map<String, String> configuration) {
        List resources = Arrays.asList(this, new CorsController());
        deployment.setResources(resources);
    }

    private void startServer(String host, int port, ResteasyDeployment deployment, Map<String, String> configuration) throws Exception {
        jetty = new org.mortbay.jetty.Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        QueuedThreadPool threadPool = new QueuedThreadPool();
        Context context = new Context(jetty, "/", Context.NO_SESSIONS);
        context.setAttribute(ResteasyDeployment.class.getName(), deployment);
        context.addServlet(new ServletHolder(new HttpServletDispatcher()), "/*");
        connector.setHost(host);
        connector.setPort(port);
        threadPool.setMaxThreads(Integer.parseInt(configuration.get(HTTP_THREADS_CONFIGURATION_PARAMETER)));
        jetty.setConnectors(new Connector[]{connector});
        jetty.setThreadPool(threadPool);
        jetty.setGracefulShutdown(500);
        jetty.setStopAtShutdown(true);
        jetty.start();
    }

}
