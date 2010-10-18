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
package terrastore.communication.protocol;

import java.util.Collections;
import java.util.Set;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Key;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;
import terrastore.store.features.Range;
import terrastore.util.collect.Sets;

/**
 * @author Sergio Bossa
 */
public class KeysInRangeCommand extends AbstractCommand<Set<Key>> {

    private final String bucketName;
    private final Range range;

    public KeysInRangeCommand(String bucketName, Range range) {
        this.bucketName = bucketName;
        this.range = range;
    }

    @Override
    public Set<Key> executeOn(Router router) throws CommunicationException, MissingRouteException, ProcessingException {
        Node node = router.routeToLocalNode();
        Command command = new KeysInRangeCommand(bucketName, range);
        return Sets.serializing(node.<Set<Key>>send(command));
    }

    public Set<Key> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            return Sets.serializing(bucket.keysInRange(range));
        } else {
            return Collections.<Key>emptySet();
        }
    }
}
