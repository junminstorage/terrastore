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
package terrastore.communication.local;

import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.communication.process.AbstractProcessor;
import terrastore.communication.process.StoreHandler;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
public class LocalProcessor extends AbstractProcessor {

    private final Store store;

    public LocalProcessor(int threads, Store store) {
        super(threads);
        this.store = store;
    }

    public <R> R process(Command<R> command) throws ProcessingException {
        return process(command, new StoreHandler<R>(store));
    }
}
