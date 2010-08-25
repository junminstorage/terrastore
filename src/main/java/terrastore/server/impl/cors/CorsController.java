package terrastore.server.impl.cors;

import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;

/**
 * This class is a Cross Origin Resource Sharing implementation.<br>
 * For more details see <a href="http://www.w3.org/TR/access-control/">CORS specification</a>.
 *
 * @author Giuseppe Santoro
 */
@Path("/")
public class CorsController {

    private static final String OK = "ok";

    @OPTIONS
    @Path("/{bucket}/{key}/{operation}")
    public String corsThreeVars() {
        return OK;
    }

    @OPTIONS
    @Path("/{bucket}/{key}")
    public String corsTwoVars() {
        return OK;
    }

    @OPTIONS
    @Path("/{bucket}")
    public String corsOneVar() {
        return OK;
    }

    @OPTIONS
    @Path("/")
    public String corsZeroVar() {
        return OK;
    }
}
