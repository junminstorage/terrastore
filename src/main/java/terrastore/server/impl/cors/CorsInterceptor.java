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
package terrastore.server.impl.cors;

import org.jboss.resteasy.annotations.interception.ServerInterceptor;
import org.jboss.resteasy.core.ResourceMethod;
import org.jboss.resteasy.core.ServerResponse;
import org.jboss.resteasy.spi.Failure;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.interception.MessageBodyWriterContext;
import org.jboss.resteasy.spi.interception.MessageBodyWriterInterceptor;
import org.jboss.resteasy.spi.interception.PreProcessInterceptor;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is a Cross Origin Resource Sharing implementation,<br>
 * it adds to the headers map a set of predefined headers to allow to the client that CORS mechanism come true.
 * For more details see <a href="http://www.w3.org/TR/access-control/">CORS specification</a>.
 *
 * @author Giuseppe Santoro
 * @author Sergio Bossa
 */
@Provider
@ServerInterceptor
public class CorsInterceptor implements PreProcessInterceptor, MessageBodyWriterInterceptor {

    /**
     * The Origin header set by the browser at each request.
     */
    private static final String ORIGIN = "Origin";
    /**
     * The Access-Control-Allow-Origin header indicates which origin a resource it is specified for can be
     * shared with. ABNF: Access-Control-Allow-Origin = "Access-Control-Allow-Origin" ":" source origin string | "*"
     */
    private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    /**
     * The Access-Control-Allow-Methods header indicates, as part of the response to a preflight request, which methods can be used during the actual request. ABNF:
     * Access-Control-Allow-Methods: "Access-Control-Allow-Methods" ":" #Method
     */
    private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    /**
     * The Access-Control-Allow-Headers header indicates, as part of the response to a preflight request, which header field names can be used during the actual request. ABNF:
     * Access-Control-Allow-Headers: "Access-Control-Allow-Headers" ":" #field-name
     */
    private static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    /**
     * The Access-Control-Max-Age header indicates how long the results of a preflight request can be cached in a preflight result cache. ABNF:
     * Access-Control-Max-Age = "Access-Control-Max-Age" ":" delta-seconds
     */
    private static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
    //
    private static final ThreadLocal<String> REQUEST_ORIGIN = new ThreadLocal<String>();
    //
    private final Set<String> allowedOrigins;
    private final String accessControlAllowMethods;
    private final String accessControlAllowHeaders;
    private final String accessControlMaxAge;

    public CorsInterceptor(String accessControlAllowOrigin, String accessControlAllowMethods, String accessControlAllowHeaders, String accessControlMaxAge) {
        this.allowedOrigins = new HashSet<String>(Arrays.asList(accessControlAllowOrigin.split(",")));
        this.accessControlAllowMethods = accessControlAllowMethods;
        this.accessControlAllowHeaders = accessControlAllowHeaders;
        this.accessControlMaxAge = accessControlMaxAge;
    }

    @Override
    public ServerResponse preProcess(HttpRequest request, ResourceMethod method) throws Failure, WebApplicationException {
        if (!allowedOrigins.isEmpty()) {
            REQUEST_ORIGIN.set("" + request.getHttpHeaders().getRequestHeaders().getFirst(ORIGIN));
        }
        return null;
    }

    @Override
    public void write(MessageBodyWriterContext context) throws IOException, WebApplicationException {
        if (!allowedOrigins.isEmpty() && (allowedOrigins.contains(REQUEST_ORIGIN.get()) || allowedOrigins.contains("*"))) {
            context.getHeaders().add(ACCESS_CONTROL_ALLOW_ORIGIN, allowedOrigins.contains("*") ? "*" : REQUEST_ORIGIN.get());
            context.getHeaders().add(ACCESS_CONTROL_ALLOW_METHODS, accessControlAllowMethods);
            context.getHeaders().add(ACCESS_CONTROL_ALLOW_HEADERS, accessControlAllowHeaders);
            context.getHeaders().add(ACCESS_CONTROL_MAX_AGE, accessControlMaxAge);
        }
        context.proceed();
    }
}
