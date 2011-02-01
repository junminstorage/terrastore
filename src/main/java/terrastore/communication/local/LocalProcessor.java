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
package terrastore.communication.local;

import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.communication.process.AbstractProcessor;
import terrastore.communication.process.RouterHandler;
import terrastore.communication.process.StoreHandler;
import terrastore.communication.process.SynchronousExecutor;
import terrastore.router.Router;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
public class LocalProcessor extends AbstractProcessor {

    private final Router router;
    private final Store store;

    public LocalProcessor(Router router, Store store) {
        super(new SynchronousExecutor());
        this.router = router;
        this.store = store;
    }

    public <R> R process(Command<R> command) throws ProcessingException {
        // If paused, it means a membership change is happening, so the command should be re-routed because partitioning may change.
        // TODO: this check-then-act is not atomic, shouldn't be a problem (just causing some additional routing) but a better solution would be great ...
        if (isPaused()) {
            return process(command, new RouterHandler<R>(router));
        } else {
            return process(command, new StoreHandler<R>(store));
        }
    }
}
