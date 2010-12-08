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
package terrastore.startup;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import terrastore.cluster.ClusterUtils;
import terrastore.cluster.coordinator.Coordinator;
import terrastore.cluster.coordinator.ServerConfiguration;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.internal.tc.MasterConnectionException;
import terrastore.internal.tc.TCMaster;
import terrastore.server.impl.JsonHttpServer;
import terrastore.util.json.JsonUtils;

/**
 * @author Sergio Bossa
 */
public class Startup {

    private static final Logger LOG = LoggerFactory.getLogger(Startup.class);
    private static final String DEFAULT_CLUSTER_NAME = "terrastore-cluster";
    private static final String DEFAULT_EVENT_BUS = "memory";
    private static final String DEFAULT_HTTP_HOST = "127.0.0.1";
    private static final int DEFAULT_HTTP_PORT = 8205;
    private static final String DEFAULT_NODE_HOST = "NULL";
    private static final int DEFAULT_NODE_PORT = 8226;
    private static final String DEFAULT_ALLOWED_ORIGINS = "*";
    private static final long DEFAULT_RECONNECT_TIMEOUT = 10000;
    private static final long DEFAULT_NODE_TIMEOUT = 10000;
    private static final int DEFAULT_HTTP_THREADS = 100;
    private static final int DEFAULT_WORKER_THREADS = 100;
    private static final int MIN_WORKER_THREADS = 32;
    private static final String DEFAULT_CONFIG_FILE = "terrastore-config.xml";
    private static final int DEFAULT_FAILOVER_RETRIES = 0;
    private static final long DEFAULT_FAILOVER_INTERVAL = 0;
    private static final boolean DEFAULT_COMPRESS_DOCUMENTS = false;
    private static final boolean DEFAULT_COMPRESS_COMMUNICATION = false;
    private static final int DEFAULT_CONCURRENCY_LEVEL = 1024;
    private static final String WELCOME_MESSAGE = "Welcome to Terrastore.";
    private static final String POWEREDBY_MESSAGE = "Powered by Terracotta (http://www.terracotta.org).";

    public static void main(String[] args) throws Exception {
        Startup startup = new Startup();
        CmdLineParser parser = new CmdLineParser(startup);
        try {
            parser.parseArgument(args);
            startup.start();
        } catch (CmdLineException ex) {
            System.err.println(ex.getMessage());
            parser.printUsage(System.err);
        }
    }

    private String master = null;
    private EnsembleConfiguration ensembleConfiguration = EnsembleConfiguration.makeDefault(DEFAULT_CLUSTER_NAME);
    private String httpHost = DEFAULT_HTTP_HOST;
    private int httpPort = DEFAULT_HTTP_PORT;
    private String nodeHost = DEFAULT_NODE_HOST;
    private int nodePort = DEFAULT_NODE_PORT;
    private long reconnectTimeout = DEFAULT_RECONNECT_TIMEOUT;
    private long nodeTimeout = DEFAULT_NODE_TIMEOUT;
    private int httpThreads = DEFAULT_HTTP_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;
    private int failoverRetries = DEFAULT_FAILOVER_RETRIES;
    private long failoverInterval = DEFAULT_FAILOVER_INTERVAL;
    private String eventBus = DEFAULT_EVENT_BUS;
    private String allowedOrigins = DEFAULT_ALLOWED_ORIGINS;
    private boolean compressDocuments = DEFAULT_COMPRESS_DOCUMENTS;
    private boolean compressCommunication = DEFAULT_COMPRESS_COMMUNICATION;
    private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;

    @Option(name = "--master", required = true)
    public void setMaster(String master) {
        this.master = master;
    }

    @Option(name = "--ensemble", required = false)
    public void setEnsemble(String ensembleConfigurationFile) throws IOException {
        this.ensembleConfiguration = JsonUtils.readEnsembleConfiguration(new FileInputStream(ensembleConfigurationFile));
        this.ensembleConfiguration.validate();
    }

    @Option(name = "--httpHost", required = false)
    public void setHttpHost(String httpHost) {
        this.httpHost = httpHost;
    }

    @Option(name = "--httpPort", required = false)
    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    @Option(name = "--nodeHost", required = false)
    public void setNodeHost(String nodeHost) {
        this.nodeHost = nodeHost;
    }

    @Option(name = "--nodePort", required = false)
    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    @Option(name = "--reconnectTimeout", required = false)
    public void setReconnectTimeout(long reconnectTimeout) {
        this.reconnectTimeout = reconnectTimeout;
    }

    @Option(name = "--nodeTimeout", required = false)
    public void setNodeTimeout(long nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
    }

    @Option(name = "--httpThreads", required = false)
    public void setHttpThreads(int httpThreads) {
        this.httpThreads = httpThreads;
    }

    @Option(name = "--workerThreads", required = false)
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    @Option(name = "--eventBus", required = false)
    public void setEventBus(String eventBus) {
        this.eventBus = eventBus;
    }

    @Option(name = "--allowedOrigins", required = false)
    public void setAllowedOrigins(String allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    @Option(name = "--failoverRetries", required = false)
    public void setFailoverRetries(int retries) {
        this.failoverRetries = retries;
    }

    @Option(name = "--failoverInterval", required = false)
    public void setFailoverInterval(int interval) {
        this.failoverInterval = interval;
    }

    @Option(name = "--compressDocs", required = false)
    public void setCompressDocuments(String compressDocuments) {
        this.compressDocuments = Boolean.parseBoolean(compressDocuments);
    }

    @Option(name = "--compressCommunication", required = false)
    public void setCompressCommunication(String compressCommunication) {
        this.compressCommunication = Boolean.parseBoolean(compressCommunication);
    }

    @Option(name = "--concurrencyLevel", required = false)
    public void setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
    }

    public void start() throws Exception {
        try {
            // TODO: make connection timeout configurable.
            if (TCMaster.getInstance().connect(master, 3, TimeUnit.MINUTES) == true) {
                verifyNodeHost();
                verifyWorkerThreads();
                printInfo();
                setupSystemParams();
                //
                ApplicationContext context = startContext();
                startCoordinator(context);
                startJsonHttpServer(context);
            } else {
                throw new MasterConnectionException("Unable to connect to master: " + master);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    private void verifyNodeHost() {
        if (nodeHost.equals(DEFAULT_NODE_HOST)) {
            nodeHost = httpHost;
        }
    }

    private void verifyWorkerThreads() {
        if (workerThreads < MIN_WORKER_THREADS) {
            workerThreads = DEFAULT_WORKER_THREADS;
        }
    }

    private void printInfo() {
        LOG.info(WELCOME_MESSAGE);
        LOG.info(POWEREDBY_MESSAGE);
        LOG.info("Listening for HTTP requests on {}:{}", httpHost, httpPort);
        LOG.info("Listening for node requests on {}:{}", nodeHost, nodePort);
        LOG.info("Reconnection timeout (in milliseconds): {}", reconnectTimeout);
        LOG.info("Node communication timeout (in milliseconds): {}", nodeTimeout);
        LOG.info("Node communication compression is {}.", compressCommunication ? "ENABLED" : "DISABLED");
        LOG.info("Document compression is {}.", compressDocuments ? "ENABLED" : "DISABLED");
        LOG.info("Failover retries: {}", failoverRetries);
        LOG.info("Failover retry interval (in milliseconds): {}", failoverInterval);
        LOG.info("Number of http threads: {}", httpThreads);
        LOG.info("Number of worker threads: {}", workerThreads);
        LOG.info("Internal concurrency level: {}", concurrencyLevel);
    }

    private void setupSystemParams() {
        // EventBus:
        if (eventBus.startsWith("amq")) {
            System.setProperty("eventBus.impl", "amq");
            System.setProperty("eventBus.amq.broker", eventBus.substring(4));
        } else {
            System.setProperty("eventBus.impl", "memory");
        }
        // Backoff configuration:
        System.setProperty("failover.retries", Integer.toString(failoverRetries));
        System.setProperty("failover.interval", Long.toString(failoverInterval));
        // Compression configuration:
        System.setProperty("compress.documents", Boolean.toString(compressDocuments));
        // Node configuration:
        System.setProperty("node.id", ClusterUtils.getServerId(TCMaster.getInstance().getClusterInfo().getCurrentNode()));
        System.setProperty("node.concurrency", Integer.toString(concurrencyLevel));
    }

    private ApplicationContext startContext() throws Exception {
        String location = getConfigFileLocation();
        ApplicationContext context = new FileSystemXmlApplicationContext(location);
        return context;
    }

    private void startCoordinator(ApplicationContext context) throws Exception {
        Coordinator coordinator = context.getBean(Coordinator.class);
        coordinator.setCompressCommunication(compressCommunication);
        coordinator.setReconnectTimeout(reconnectTimeout);
        coordinator.setNodeTimeout(nodeTimeout);
        coordinator.setWokerThreads(workerThreads);
        coordinator.start(
                new ServerConfiguration(ClusterUtils.getServerId(TCMaster.getInstance().getClusterInfo().getCurrentNode()), nodeHost, nodePort, httpHost, httpPort),
                ensembleConfiguration);
    }

    private void startJsonHttpServer(ApplicationContext context) throws Exception {
        JsonHttpServer server = context.getBean(JsonHttpServer.class);
        Map<String, String> configuration = new HashMap<String, String>();
        configuration.put(JsonHttpServer.CORS_ALLOWED_ORIGINS_CONFIGURATION_PARAMETER, allowedOrigins);
        configuration.put(JsonHttpServer.HTTP_THREADS_CONFIGURATION_PARAMETER, Integer.toString(httpThreads));
        server.start(httpHost, httpPort, configuration);
    }

    private String getConfigFileLocation() {
        String homeDir = System.getenv(Constants.TERRASTORE_HOME) != null ? System.getenv(Constants.TERRASTORE_HOME) : System.getProperty(Constants.TERRASTORE_HOME);
        if (homeDir != null) {
            String separator = System.getProperty("file.separator");
            String location = "file:" + homeDir + separator + DEFAULT_CONFIG_FILE;
            return location;
        } else {
            throw new IllegalStateException("Terrastore home directory is not set!");
        }
    }

}
