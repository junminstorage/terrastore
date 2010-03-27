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
package terrastore.communication.seda;

import terrastore.communication.protocol.Command;
import terrastore.router.Router;

/**
 * @author Sergio Bossa
 */
public class RoutingHandler<R> implements CommandHandler<R> {

    private final Router router;

    public RoutingHandler(Router router) {
        this.router = router;
    }

    @Override
    public R handle(Command<R> command) throws Exception {
        return command.route(router);
    }
}
