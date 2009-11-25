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
    private static final int DEFAULT_HTTP_PORT = 8080;
    private static final int DEFAULT_SHUTDOWN_PORT = 8180;
    private static final String DEFAULT_SHUTDOWN_KEY = "terrastore";
    private static final long DEFAULT_NODE_TIMEOUT = 1000;
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
    private int httpPort = DEFAULT_HTTP_PORT;
    private int shutdownPort = DEFAULT_SHUTDOWN_PORT;
    private long nodeTimeout = DEFAULT_NODE_TIMEOUT;
    private int workerThreads = DEFAULT_WORKER_THREADS;
    private String configFile = DEFAULT_CONFIG_FILE;

    @Option(name = "--httpPort", required = false)
    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    @Option(name = "--shutdownPort", required = false)
    public void setShutdownPort(int shutdownPort) {
        this.shutdownPort = shutdownPort;
    }

    @Option(name = "--nodeTimeout", required = false)
    public void setNodeTimeout(long nodeTimeout) {
        this.nodeTimeout = nodeTimeout;
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
            LOG.info(WELCOME_MESSAGE);
            LOG.info(POWEREDBY_MESSAGE);
            Context context = startServer();
            startMonitor();
            startCluster(context);
            setupClusterShutdownHook(context);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private Context startServer() throws Exception {
        Server server = new Server();
        Connector connector = new SelectChannelConnector();
        Context context = new Context(server, "/", Context.NO_SESSIONS);
        context.setInitParams(getContextParams());
        context.addEventListener(new ResteasyBootstrap());
        context.addEventListener(new SpringContextLoaderListener());
        context.addServlet(new ServletHolder(new HttpServletDispatcher()), "/*");
        connector.setPort(httpPort);
        server.setConnectors(new Connector[]{connector});
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
        cluster.start();
    }

    private void setupClusterShutdownHook(Context context) throws BeansException {
        final Cluster cluster = getClusterFromServletContext(context);
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                cluster.stop();
            }
        });
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
        String homeDir = System.getenv(TERRASTORE_HOME_DIR) != null ? System.getenv(TERRASTORE_HOME_DIR) : System.getProperty(TERRASTORE_HOME_DIR);
        if (homeDir != null) {
            String separator = System.getProperty("file.separator");
            String url = "file:" + homeDir + separator + configFile;
            return url;
        } else {
            throw new IllegalStateException("TCStore home directory is not set!");
        }
    }
}
