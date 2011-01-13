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
package terrastore.test.embedded;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import terrastore.communication.NodeConfiguration;
import terrastore.communication.Cluster;
import terrastore.communication.Node;
import terrastore.communication.local.LocalNode;
import terrastore.communication.local.LocalProcessor;
import terrastore.router.Router;
import terrastore.server.impl.JsonHttpServer;
import terrastore.store.Store;
import terrastore.util.collect.Maps;
import terrastore.util.collect.Sets;

/**
 * @author Sergio Bossa
 */
public class TerrastoreEmbeddedServer {

    private ApplicationContext context;

    public synchronized void start(String httpHost, int httpPort) throws Exception {
        context = new ClassPathXmlApplicationContext("terrastore/test/embedded/terrastore-config.xml");
        configureCluster((Router) context.getBean("router"), (Store) context.getBean("store"), httpHost, httpPort);
        startJsonHttpServer((JsonHttpServer) context.getBean("jsonHttpServer"), httpHost, httpPort);
    }

    public synchronized void stop() throws Exception {
        if (context != null) {
            stopServer((JsonHttpServer) context.getBean("jsonHttpServer"));
        } else {
            throw new IllegalStateException("Server is not started!");
        }
    }

    private void configureCluster(Router router, Store store, String httpHost, int httpPort) {
        NodeConfiguration configuration = new NodeConfiguration("in-memory-terrastore", httpHost, -1, httpHost, httpPort);
        Cluster localCluster = new Cluster("in-memory-cluster", true);
        Node localNode = new LocalNode.Factory().makeLocalNode(configuration, new LocalProcessor(router, store));
        router.setupClusters(Sets.hash(localCluster));
        router.addRouteToLocalNode(localNode);
        router.addRouteTo(localCluster, localNode);
    }

    private void startJsonHttpServer(JsonHttpServer server, String httpHost, int httpPort) throws Exception {
        server.start(httpHost, httpPort, Maps.hash(new String[]{JsonHttpServer.CORS_ALLOWED_ORIGINS_CONFIGURATION_PARAMETER, JsonHttpServer.HTTP_THREADS_CONFIGURATION_PARAMETER}, new String[]{"*", "10"}));
    }

    private void stopServer(JsonHttpServer server) throws Exception {
        server.stop();
    }
}
