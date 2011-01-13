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
package terrastore.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import terrastore.common.ErrorLogger;
import terrastore.communication.Cluster;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.KeysInRangeCommand;
import terrastore.router.Router;
import terrastore.service.KeyRangeService;
import terrastore.store.Key;
import terrastore.store.features.Range;
import terrastore.util.collect.parallel.MapCollector;
import terrastore.util.collect.parallel.MapTask;
import terrastore.util.collect.parallel.ParallelExecutionException;
import terrastore.util.collect.parallel.ParallelUtils;
import terrastore.util.concurrent.GlobalExecutor;

/**
 * @author Sven Johansson
 * @author Sergio Bossa
 */
public class DefaultKeyRangeService implements KeyRangeService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultKeyRangeService.class);
    //
    private final Router router;

    public DefaultKeyRangeService(Router router) {
        this.router = router;
    }

    @Override
    public Set<Key> getKeyRangeForBucket(String bucket, Range keyRange) throws ParallelExecutionException {
        KeysInRangeCommand command = new KeysInRangeCommand(bucket, keyRange);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<Key> keys = multicastRangeQueryCommand(perClusterNodes, command);
        return keys;
    }

    private Set<Key> multicastRangeQueryCommand(final Map<Cluster, Set<Node>> perClusterNodes, final KeysInRangeCommand command) throws ParallelExecutionException {
        // Parallel collection of all sets of sorted keys in a list:
        Set<Key> keys = ParallelUtils.parallelMap(
                perClusterNodes.values(),
                new MapTask<Set<Node>, Set<Key>>() {

                    @Override
                    public Set<Key> map(Set<Node> nodes) throws ParallelExecutionException {
                        Set<Key> keys = new HashSet<Key>();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                keys = node.<Set<Key>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (CommunicationException ex) {
                                ErrorLogger.LOG(LOG, ex.getErrorMessage(), ex);
                            } catch (ProcessingException ex) {
                                ErrorLogger.LOG(LOG, ex.getErrorMessage(), ex);
                                throw new ParallelExecutionException(ex);
                            }
                        }
                        return keys;
                    }

                },
                new MapCollector<Set<Key>, Set<Key>>() {

                    @Override
                    public Set<Key> collect(List<Set<Key>> keys) {
                        try {
                            // Parallel merge of all sorted sets:
                            return ParallelUtils.parallelMerge(keys, GlobalExecutor.getForkJoinPool());
                        } catch (ParallelExecutionException ex) {
                            throw new IllegalStateException(ex.getCause());
                        }
                    }

                }, GlobalExecutor.getQueryExecutor());
        return keys;
    }

}
