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

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.jboss.resteasy.plugins.spring.SpringContextLoaderListener;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.start.Monitor;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.web.context.support.WebApplicationContextUtils;
import terrastore.cluster.coordinator.Coordinator;
import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.cluster.ensemble.EnsembleConfigurationUtils;

/**
 * @author Sergio Bossa
 */
public class Startup {

    private static final Logger LOG = LoggerFactory.getLogger(Startup.class);
    private static final String DEFAULT_CLUSTER_NAME = "terrastore-cluster";
    private static final String DEFAULT_EVENT_BUS = "memory";
    private static final String DEFAULT_HTTP_HOST = "127.0.0.1";
    private static final int DEFAULT_HTTP_PORT = 8080;
    private static final String DEFAULT_NODE_HOST = "NULL";
    private static final int DEFAULT_NODE_PORT = 8226;
    private static final int DEFAULT_SHUTDOWN_PORT = 8180;
    private static final String DEFAULT_SHUTDOWN_KEY = "terrastore";
    private static final String DEFAULT_ALLOWED_ORIGINS = "";
    private static final long DEFAULT_RECONNECT_TIMEOUT = 10000;
    private static final long DEFAULT_NODE_TIMEOUT = 10000;
    private static final int DEFAULT_HTTP_THREADS = 100;
    private static final int MIN_WORKER_THREADS = 10;
    private static final int DEFAULT_WORKER_THREADS = Runtime.getRuntime().availableProcessors() * 10;
    private static final String DEFAULT_CONFIG_FILE = "terrastore-config.xml";
    private static final String WELCOME_MESSAGE = "Welcome to Terrastore.";
    private static final String POWEREDBY_MESSAGE = "Powered by Terracotta (http://www.terracotta.org).";

    public static void main(String[] args) {
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

    public static void shutdown(String host, int shutdownPort) {
        Socket shutdownSocket = null;
        try {
            shutdownSocket = new Socket(InetAddress.getByName(host), shutdownPort);
            BufferedWriter outStream = new BufferedWriter(new OutputStreamWriter(shutdownSocket.getOutputStream()));
            outStream.write(DEFAULT_SHUTDOWN_KEY);
            outStream.newLine();
            outStream.write("stop");
            outStream.newLine();
            outStream.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            if (shutdownSocket != null) {
                try {
                    shutdownSocket.close();
                } catch (IOException innerEx) {
                    innerEx.printStackTrace();
                }
            }
        }
    }
    //
    private EnsembleConfiguration ensembleConfiguration = EnsembleConfigurationUtils.makeDefault(DEFAULT_CLUSTER_NAME);
    private String httpHost = DEFAULT_HTTP_HOST;
    private int httpPort = DEFAULT_HTTP_PORT;
    private String nodeHost = DEFAULT_NODE_HOST;
    private int nodePort = DEFAULT_NODE_PORT;
    private int shutdownPort = DEFAULT_SHUTDOWN_PORT;
    private long reconnectTimeout = DEFAULT_RECONNECT_TIMEOUT;
    private long nodeTimeout = DEFAULT_NODE_TIMEOUT;
    private int httpThreads = DEFAULT_HTTP_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;
    private String eventBus = DEFAULT_EVENT_BUS;
    private String allowedOrigins = DEFAULT_ALLOWED_ORIGINS;

    @Option(name = "--master", required = false)
    public void setMaster(String toIgnore) {
        // Ignore this, here just to let the master to be passed by command line.
    }

    @Option(name = "--ensemble", required = false)
    public void setEnsemble(String ensembleConfigurationFile) throws IOException {
        this.ensembleConfiguration = EnsembleConfigurationUtils.readFrom(new FileInputStream(ensembleConfigurationFile));
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

    @Option(name = "--shutdownPort", required = false)
    public void setShutdownPort(int shutdownPort) {
        this.shutdownPort = shutdownPort;
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

    public void start() {
        try {
            verifyNodeHost();
            verifyWorkerThreads();
            printInfo();
            Context context = startServer();
            startMonitor();
            startCoordinator(context);
        } catch (Exception ex) {
            ex.printStackTrace();
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
        LOG.info("Reconnection timeout (in milliseconds) set to {}", reconnectTimeout);
        LOG.info("Node communication timeout (in milliseconds) set to {}", nodeTimeout);
        LOG.info("Number of http threads: {}", httpThreads);
        LOG.info("Number of worker threads: {}", workerThreads);
    }

    private Context startServer() throws Exception {
        Server server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        QueuedThreadPool threadPool = new QueuedThreadPool();
        Context context = new Context(server, "/", Context.NO_SESSIONS);
        context.setInitParams(getContextParams());
        context.addEventListener(new ResteasyBootstrap());
        context.addEventListener(new SpringContextLoaderListener());
        context.addServlet(new ServletHolder(new HttpServletDispatcher()), "/*");
        connector.setHost(httpHost);
        connector.setPort(httpPort);
        threadPool.setMaxThreads(httpThreads);
        server.setConnectors(new Connector[]{connector});
        server.setThreadPool(threadPool);
        server.setGracefulShutdown(500);
        server.setStopAtShutdown(true);
        server.start();
        return context;
    }

    private void startMonitor() {
        System.setProperty("STOP.PORT", Integer.toString(shutdownPort));
        System.setProperty("STOP.KEY", DEFAULT_SHUTDOWN_KEY);
        Monitor.monitor();
    }

    private void startCoordinator(Context context) throws BeansException {
        Coordinator coordinator = getCoordinatorFromServletContext(context);
        coordinator.setReconnectTimeout(reconnectTimeout);
        coordinator.setNodeTimeout(nodeTimeout);
        coordinator.setWokerThreads(workerThreads);
        coordinator.start(nodeHost, nodePort, ensembleConfiguration);
    }

    private Coordinator getCoordinatorFromServletContext(Context context) throws BeansException {
        Map beans = WebApplicationContextUtils.getWebApplicationContext(context.getServletContext()).getBeansOfType(Coordinator.class);
        if (beans.size() == 1) {
            return (Coordinator) beans.values().iterator().next();
        } else {
            throw new IllegalStateException("Wrong number of configured beans!");
        }
    }

    private Map<String, String> getContextParams() {
        Map<String, String> contextParams = new HashMap<String, String>();
        // Spring context location:
        contextParams.put("contextConfigLocation", getConfigFileLocation());
        // Resteasy providers:
        contextParams.put("resteasy.use.builtin.providers", "false");
        // EventBus:
        if (eventBus.startsWith("amq")) {
            contextParams.put("eventBus.impl", "amq");
            contextParams.put("eventBus.amq.broker", eventBus.substring(4));
        } else {
            contextParams.put("eventBus.impl", "memory");
        }
        // Allowed hosts for CORS:
        contextParams.put("cors.allowed.origins", allowedOrigins);
        return contextParams;
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
