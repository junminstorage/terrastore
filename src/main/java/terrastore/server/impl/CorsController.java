package terrastore.server.impl;

import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
/**
 * This class is a Cross Origin Resource Sharing implementation,<br>
 * it allows the preflight request and a CORS mechanism for not base methods(GET, POST) come true.
 * For more details see <a href="http://www.w3.org/TR/access-control/">CORS specification</a>.
 * @author Giuseppe Santoro
 *
 */
@Path("/")
public class CorsController {

    @OPTIONS
    @Path("/{bucket}/{key}/{operation}")
    public String corsThreeVars() {
        return "ok";
    }

    @OPTIONS
    @Path("/{bucket}/{key}")
    public String corsTwoVars() {
        return "ok";
    }

    @OPTIONS
    @Path("/{bucket}")
    public String corsOneVar() {
        return "ok";
    }

    @OPTIONS
    @Path("/")
    public String corsZeroVar() {
        return "ok";
    }
}