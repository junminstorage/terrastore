/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
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
