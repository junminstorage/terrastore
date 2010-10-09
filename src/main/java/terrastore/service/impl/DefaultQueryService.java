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
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.GetKeysCommand;
import terrastore.communication.protocol.RangeQueryCommand;
import terrastore.communication.protocol.GetBucketsCommand;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.util.collect.Maps;
import terrastore.util.collect.parallel.ParallelUtils;
import terrastore.util.collect.Sets;
import terrastore.util.collect.parallel.MapCollector;
import terrastore.util.collect.parallel.MapTask;
import terrastore.util.collect.parallel.ParallelExecutionException;

/**
 * @author Sergio Bossa
 */
public class DefaultQueryService implements QueryService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueryService.class);
    private final Router router;

    public DefaultQueryService(Router router) {
        this.router = router;
    }

    @Override
    public Set<String> getBuckets() throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Getting bucket names.");
            GetBucketsCommand command = new GetBucketsCommand();
            Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
            Set<String> buckets = multicastGetBucketsCommand(perClusterNodes, command);
            return buckets;
        } catch (ParallelExecutionException ex) {
            if (ex.getCause() instanceof ProcessingException) {
                ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
                ErrorLogger.LOG(LOG, error, ex.getCause());
                throw new QueryOperationException(error);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
            }
        }
    }

    @Override
    public Value getValue(String bucket, Key key, Predicate predicate) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Getting value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            GetValueCommand command = null;
            if (predicate == null || predicate.isEmpty()) {
                command = new GetValueCommand(bucket, key);
            } else {
                command = new GetValueCommand(bucket, key, predicate);
            }
            Value result = node.<Value>send(command);
            return result;
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ProcessingException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<Key, Value> getAllValues(final String bucket, final int limit) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Getting all values from bucket {}", bucket);
            Set<Key> allKeys = Sets.limited(getAllKeysForBucket(bucket), limit);
            Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, allKeys);
            List<Map<Key, Value>> allKeyValues = ParallelUtils.parallelMap(
                    nodeToKeys.entrySet(),
                    new MapTask<Map.Entry<Node, Set<Key>>, Map<Key, Value>>() {

                        @Override
                        public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                GetValuesCommand command = new GetValuesCommand(bucket, keys);
                                return node.<Map<Key, Value>>send(command);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    },
                    new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                        @Override
                        public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                            return allKeyValues;
                        }
                    });
            return Maps.union(allKeyValues);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ParallelExecutionException ex) {
            if (ex.getCause() instanceof ProcessingException) {
                ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
                ErrorLogger.LOG(LOG, error, ex.getCause());
                throw new QueryOperationException(error);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
            }
        }
    }

    @Override
    public Map<Key, Value> queryByRange(final String bucket, final Range range, final Predicate predicate, final long timeToLive) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Range query on bucket {}", bucket);
            Set<Key> keysInRange = Sets.limited(getKeyRangeForBucket(bucket, range, timeToLive), range.getLimit());
            Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, keysInRange);
            List<Map<Key, Value>> allKeyValues = ParallelUtils.parallelMap(
                    nodeToKeys.entrySet(),
                    new MapTask<Map.Entry<Node, Set<Key>>, Map<Key, Value>>() {

                        @Override
                        public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                GetValuesCommand command = null;
                                if (predicate.isEmpty()) {
                                    command = new GetValuesCommand(bucket, keys);
                                } else {
                                    command = new GetValuesCommand(bucket, keys, predicate);
                                }
                                return node.<Map<Key, Value>>send(command);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    },
                    new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                        @Override
                        public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                            return allKeyValues;
                        }
                    });
            return Maps.composite(keysInRange, allKeyValues);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ParallelExecutionException ex) {
            if (ex.getCause() instanceof ProcessingException) {
                ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
                ErrorLogger.LOG(LOG, error, ex.getCause());
                throw new QueryOperationException(error);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
            }
        }
    }

    @Override
    public Map<Key, Value> queryByPredicate(final String bucket, final Predicate predicate) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Predicate-based query on bucket {}", bucket);
                Set<Key> allKeys = getAllKeysForBucket(bucket);
                Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, allKeys);
                List<Map<Key, Value>> allKeyValues = ParallelUtils.parallelMap(
                        nodeToKeys.entrySet(),
                        new MapTask<Map.Entry<Node, Set<Key>>, Map<Key, Value>>() {

                            @Override
                            public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) {
                                try {
                                    Node node = nodeToKeys.getKey();
                                    Set<Key> keys = nodeToKeys.getValue();
                                    GetValuesCommand command = new GetValuesCommand(bucket, keys, predicate);
                                    return node.<Map<Key, Value>>send(command);
                                } catch (Exception ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        },
                        new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                            @Override
                            public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                                return allKeyValues;
                            }
                        });
                return Maps.union(allKeyValues);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ParallelExecutionException ex) {
            if (ex.getCause() instanceof ProcessingException) {
                ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
                ErrorLogger.LOG(LOG, error, ex.getCause());
                throw new QueryOperationException(error);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
            }
        }
    }

    @Override
    public Router getRouter() {
        return router;
    }

    private Set<Key> getAllKeysForBucket(String bucket) throws ParallelExecutionException {
        GetKeysCommand command = new GetKeysCommand(bucket);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<Key> keys = multicastGetAllKeysCommand(perClusterNodes, command);
        return keys;
    }

    private Set<Key> getKeyRangeForBucket(String bucket, Range keyRange, long timeToLive) throws ParallelExecutionException {
        RangeQueryCommand command = new RangeQueryCommand(bucket, keyRange, timeToLive);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<Key> keys = multicastRangeQueryCommand(perClusterNodes, command);
        return keys;
    }

    private Set<String> multicastGetBucketsCommand(final Map<Cluster, Set<Node>> perClusterNodes, final GetBucketsCommand command) throws ParallelExecutionException {
        // Parallel collection of all buckets:
        Set<String> result = ParallelUtils.parallelMap(
                perClusterNodes.values(),
                new MapTask<Set<Node>, Set<String>>() {

                    @Override
                    public Set<String> map(Set<Node> nodes) {
                        Set<String> buckets = new HashSet<String>();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                buckets = node.<Set<String>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (Exception ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return buckets;
                    }
                },
                new MapCollector<Set<String>, Set<String>>() {

                    @Override
                    public Set<String> collect(List<Set<String>> keys) {
                        return Sets.union(keys);
                    }
                });
        return result;
    }

    private Set<Key> multicastGetAllKeysCommand(final Map<Cluster, Set<Node>> perClusterNodes, final GetKeysCommand command) throws ParallelExecutionException {
        // Parallel collection of all keys:
        Set<Key> result = ParallelUtils.parallelMap(
                perClusterNodes.values(),
                new MapTask<Set<Node>, Set<Key>>() {

                    @Override
                    public Set<Key> map(Set<Node> nodes) {
                        Set<Key> keys = new HashSet<Key>();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                keys = node.<Set<Key>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (Exception ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return keys;
                    }
                },
                new MapCollector<Set<Key>, Set<Key>>() {

                    @Override
                    public Set<Key> collect(List<Set<Key>> keys) {
                        return Sets.union(keys);
                    }
                });
        return result;
    }

    private Set<Key> multicastRangeQueryCommand(final Map<Cluster, Set<Node>> perClusterNodes, final RangeQueryCommand command) throws ParallelExecutionException {
        // Parallel collection of all sets of sorted keys in a list:
        Set<Key> keys = ParallelUtils.parallelMap(
                perClusterNodes.values(),
                new MapTask<Set<Node>, Set<Key>>() {

                    @Override
                    public Set<Key> map(Set<Node> nodes) {
                        Set<Key> keys = new HashSet<Key>();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                keys = node.<Set<Key>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (Exception ex) {
                                LOG.error(ex.getMessage(), ex);
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
                            return ParallelUtils.parallelMerge(keys);
                        } catch (ParallelExecutionException ex) {
                            throw new IllegalStateException(ex.getCause());
                        }
                    }
                });
        return keys;
    }
}
