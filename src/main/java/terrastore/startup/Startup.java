/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
import terrastore.cluster.Cluster;

/**
 * @author Sergio Bossa
 */
public class Startup {

    private static final Logger LOG = LoggerFactory.getLogger(Startup.class);
    private static final String DEFAULT_HTTP_HOST = "127.0.0.1";
    private static final int DEFAULT_HTTP_PORT = 8080;
    private static final String DEFAULT_NODE_HOST = "127.0.0.1";
    private static final int DEFAULT_NODE_PORT = 8226;
    private static final int DEFAULT_SHUTDOWN_PORT = 8180;
    private static final String DEFAULT_SHUTDOWN_KEY = "terrastore";
    private static final long DEFAULT_NODE_TIMEOUT = 10000;
    private static final int DEFAULT_HTTP_THREADS = 100;
    private static final int DEFAULT_WORKER_THREADS = Runtime.getRuntime().availableProcessors() * 10;
    private static final String DEFAULT_CONFIG_FILE = "terrastore-config.xml";
    private static final String TERRASTORE_HOME_DIR = "TERRASTORE_HOME";
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

    private String httpHost = DEFAULT_HTTP_HOST;
    private int httpPort = DEFAULT_HTTP_PORT;
    private String nodeHost = DEFAULT_NODE_HOST;
    private int nodePort = DEFAULT_NODE_PORT;
    private int shutdownPort = DEFAULT_SHUTDOWN_PORT;
    private long nodeTimeout = DEFAULT_NODE_TIMEOUT;
    private int httpThreads = DEFAULT_HTTP_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;
    private String configFile;

    @Option(name = "--master", required = false)
    public void setMaster(String toIgnore) {
        // Ignore this, here just to let the master to be passed by command line.
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

    @Option(name = "--configFile", required = false)
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void start() {
        try {
            printInfo();
            Context context = startServer();
            startMonitor();
            startCluster(context);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void printInfo() {
        LOG.info(WELCOME_MESSAGE);
        LOG.info(POWEREDBY_MESSAGE);
        LOG.info("Listening for HTTP requests on {}:{}", httpHost, httpPort);
        LOG.info("Listening for node requests on {}:{}", nodeHost, nodePort);
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

    private void startCluster(Context context) throws BeansException {
        Cluster cluster = getClusterFromServletContext(context);
        cluster.setNodeTimeout(nodeTimeout);
        cluster.setWokerThreads(workerThreads);
        cluster.start(nodeHost, nodePort);
    }

    private Cluster getClusterFromServletContext(Context context) throws BeansException {
        Map clusterBeans = WebApplicationContextUtils.getWebApplicationContext(context.getServletContext()).getBeansOfType(Cluster.class);
        if (clusterBeans.size() == 1) {
            return (Cluster) clusterBeans.values().iterator().next();
        } else {
            throw new IllegalStateException("Wrong number of configured cluster beans!");
        }
    }

    private Map<String, String> getContextParams() {
        Map<String, String> contextParams = new HashMap<String, String>();
        contextParams.put("contextConfigLocation", getConfigFileLocation());
        contextParams.put("resteasy.use.builtin.providers", "false");
        return contextParams;
    }

    private String getConfigFileLocation() {
        String location = null;
        if (configFile != null) {
            location = "file:" + configFile;
        } else {
            String homeDir = System.getenv(TERRASTORE_HOME_DIR) != null ? System.getenv(TERRASTORE_HOME_DIR) : System.getProperty(TERRASTORE_HOME_DIR);
            if (homeDir != null) {
                String separator = System.getProperty("file.separator");
                location = "file:" + homeDir + separator + DEFAULT_CONFIG_FILE;
            } else {
                throw new IllegalStateException("TCStore home directory is not set!");
            }
        }
        return location;
    }
}
