package terrastore.server.impl.support;

import org.jboss.resteasy.annotations.interception.ServerInterceptor;
import org.jboss.resteasy.spi.interception.MessageBodyWriterContext;
import org.jboss.resteasy.spi.interception.MessageBodyWriterInterceptor;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * This class is a Cross Origin Resource Sharing implementation,<br>
 * it adds to the headers map a set of predefined headers to allow to the client that CORS mechanism come true.
 * For more details see <a href="http://www.w3.org/TR/access-control/">CORS specification</a>.
 *
 * @author Giuseppe Santoro
 */
@Provider
@ServerInterceptor
public class CorsInterceptor implements MessageBodyWriterInterceptor {

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

    private final String accessControlAllowOrigin;
    private final String accessControlAllowMethods;
    private final String accessControlAllowHeaders;
    private final String accessControlMaxAge;

    public CorsInterceptor(String accessControlAllowOrigin, String accessControlAllowMethods, String accessControlAllowHeaders, String accessControlMaxAge) {
        this.accessControlAllowOrigin = accessControlAllowOrigin;
        this.accessControlAllowMethods = accessControlAllowMethods;
        this.accessControlAllowHeaders = accessControlAllowHeaders;
        this.accessControlMaxAge = accessControlMaxAge;
    }

    public void write(MessageBodyWriterContext context) throws IOException, WebApplicationException {
        context.getHeaders().add(ACCESS_CONTROL_ALLOW_ORIGIN, accessControlAllowOrigin);
        context.getHeaders().add(ACCESS_CONTROL_ALLOW_METHODS, accessControlAllowMethods);
        context.getHeaders().add(ACCESS_CONTROL_ALLOW_HEADERS, accessControlAllowHeaders);
        context.getHeaders().add(ACCESS_CONTROL_MAX_AGE, accessControlMaxAge);
        context.proceed();
    }
}
