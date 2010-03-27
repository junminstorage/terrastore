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
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.store.Bucket;
import terrastore.store.Store;
import terrastore.store.StoreOperationException;

/**
 * @author Sergio Bossa
 */
public class GetKeysCommand extends AbstractCommand<Set<String>> {

    private final String bucketName;

    public GetKeysCommand(String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public Set<String> route(Router router) throws MissingRouteException, ProcessingException {
        Node node = router.routeToLocalNode();
        return node.<Set<String>>send(this);
    }

    public Set<String> executeOn(Store store) throws StoreOperationException {
        Bucket bucket = store.get(bucketName);
        if (bucket != null) {
            return bucket.keys();
        } else {
            return Collections.emptySet();
        }
    }
}
