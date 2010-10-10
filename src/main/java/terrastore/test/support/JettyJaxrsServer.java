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
package terrastore.test.support;

import org.jboss.resteasy.plugins.server.embedded.EmbeddedJaxrsServer;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * @author Sergio Bossa
 */
public class JettyJaxrsServer implements EmbeddedJaxrsServer {

    private String rootResourcePath;
    private SecurityDomain securityDomain;
    private ResteasyDeployment deployment;
    private final org.mortbay.jetty.Server jetty;
    private final String host;
    private final int port;

    public JettyJaxrsServer(String host, int port) {
        this.rootResourcePath = "/";
        this.deployment = new ResteasyDeployment();
        this.jetty = new org.mortbay.jetty.Server();
        this.host = host;
        this.port = port;
    }

    public synchronized void start() {
        try {
            SelectChannelConnector connector = new SelectChannelConnector();
            Context context = new Context(jetty, rootResourcePath, Context.NO_SESSIONS);
            context.addServlet(new ServletHolder(new HttpServletDispatcher()), "/*");
            context.setAttribute(ResteasyDeployment.class.getName(), deployment);
            connector.setHost(host);
            connector.setPort(port);
            jetty.setConnectors(new Connector[]{connector});
            jetty.setGracefulShutdown(500);
            jetty.setStopAtShutdown(true);
            jetty.start();
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot start embedded server!");
        }
    }

    @Override
    public synchronized void stop() {
        try {
            jetty.stop();
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot stop embedded server!");
        }
    }

    @Override
    public ResteasyDeployment getDeployment() {
        return deployment;
    }

    @Override
    public void setDeployment(ResteasyDeployment deployment) {
        this.deployment = deployment;
    }

    @Override
    public void setSecurityDomain(SecurityDomain sc) {
        this.securityDomain = securityDomain;
    }

    @Override
    public void setRootResourcePath(String rootResourcePath) {
        this.rootResourcePath = rootResourcePath;
    }
}
