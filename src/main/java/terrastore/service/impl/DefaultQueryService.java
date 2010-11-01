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
import terrastore.communication.protocol.KeysInRangeCommand;
import terrastore.communication.protocol.GetBucketsCommand;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.communication.protocol.MapCommand;
import terrastore.communication.protocol.ReduceCommand;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.features.Reducer;
import terrastore.util.collect.Maps;
import terrastore.util.collect.parallel.ParallelUtils;
import terrastore.util.collect.Sets;
import terrastore.util.collect.parallel.MapCollector;
import terrastore.util.collect.parallel.MapTask;
import terrastore.util.collect.parallel.ParallelExecutionException;
import terrastore.util.global.GlobalExecutor;

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
            handleParallelExecutionException(ex);
            return null;
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
            handleMissingRouteException(ex);
            return null;
        } catch (ProcessingException ex) {
            handleProcessingException(ex);
            return null;
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
                        public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) throws ParallelExecutionException {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                GetValuesCommand command = new GetValuesCommand(bucket, keys);
                                return node.<Map<Key, Value>>send(command);
                            } catch (Exception ex) {
                                throw new ParallelExecutionException(ex);
                            }
                        }

                    },
                    new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                        @Override
                        public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                            return allKeyValues;
                        }

                    }, GlobalExecutor.getServiceExecutor());
            return Maps.union(allKeyValues);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ParallelExecutionException ex) {
            handleParallelExecutionException(ex);
            return null;
        }
    }

    @Override
    public Map<Key, Value> queryByRange(final String bucket, final Range range, final Predicate predicate) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("Range query on bucket {}", bucket);
            Set<Key> keysInRange = Sets.limited(getKeyRangeForBucket(bucket, range), range.getLimit());
            Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, keysInRange);
            List<Map<Key, Value>> allKeyValues = ParallelUtils.parallelMap(
                    nodeToKeys.entrySet(),
                    new MapTask<Map.Entry<Node, Set<Key>>, Map<Key, Value>>() {

                        @Override
                        public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) throws ParallelExecutionException {
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
                                throw new ParallelExecutionException(ex);
                            }
                        }

                    },
                    new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                        @Override
                        public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                            return allKeyValues;
                        }

                    }, GlobalExecutor.getServiceExecutor());
            return Maps.composite(keysInRange, allKeyValues);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ParallelExecutionException ex) {
            handleParallelExecutionException(ex);
            return null;
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
                        public Map<Key, Value> map(Map.Entry<Node, Set<Key>> nodeToKeys) throws ParallelExecutionException {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                GetValuesCommand command = new GetValuesCommand(bucket, keys, predicate);
                                return node.<Map<Key, Value>>send(command);
                            } catch (Exception ex) {
                                throw new ParallelExecutionException(ex);
                            }
                        }

                    },
                    new MapCollector<Map<Key, Value>, List<Map<Key, Value>>>() {

                        @Override
                        public List<Map<Key, Value>> collect(List<Map<Key, Value>> allKeyValues) {
                            return allKeyValues;
                        }

                    }, GlobalExecutor.getServiceExecutor());
            return Maps.union(allKeyValues);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ParallelExecutionException ex) {
            handleParallelExecutionException(ex);
            return null;
        }
    }

    @Override
    public Value queryByMapReduce(final String bucket, final Range range, final Mapper mapper, final Reducer reducer) throws CommunicationException, QueryOperationException {
        try {
            LOG.debug("MapReduce query on bucket {}", bucket);
            Set<Key> keys = null;
            if (range != null && !range.isEmpty()) {
                keys = getKeyRangeForBucket(bucket, range);
            } else {
                keys = getAllKeysForBucket(bucket);
            }
            Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, keys);
            //
            // Map:
            List<Map<String, Object>> mapResults = ParallelUtils.parallelMap(
                    nodeToKeys.entrySet(),
                    new MapTask<Map.Entry<Node, Set<Key>>, Map<String, Object>>() {

                        @Override
                        public Map<String, Object> map(Map.Entry<Node, Set<Key>> nodeToKeys) throws ParallelExecutionException {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                MapCommand command = new MapCommand(bucket, keys, mapper);
                                return node.<Map<String, Object>>send(command);
                            } catch (Exception ex) {
                                throw new ParallelExecutionException(ex);
                            }
                        }

                    },
                    new MapCollector<Map<String, Object>, List<Map<String, Object>>>() {

                        @Override
                        public List<Map<String, Object>> collect(List<Map<String, Object>> values) {
                            return values;
                        }

                    }, GlobalExecutor.getServiceExecutor());
            //
            // Reduce:
            Node reducerNode = router.routeToLocalNode();
            ReduceCommand reducerCommand = new ReduceCommand(mapResults, reducer);
            return reducerNode.<Value>send(reducerCommand);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ParallelExecutionException ex) {
            handleParallelExecutionException(ex);
            return null;
        } catch (ProcessingException ex) {
            handleProcessingException(ex);
            return null;
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

    private Set<Key> getKeyRangeForBucket(String bucket, Range keyRange) throws ParallelExecutionException {
        KeysInRangeCommand command = new KeysInRangeCommand(bucket, keyRange);
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
                    public Set<String> map(Set<Node> nodes) throws ParallelExecutionException {
                        Set<String> buckets = new HashSet<String>();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                buckets = node.<Set<String>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (CommunicationException ex) {
                                LOG.error(ex.getMessage(), ex);
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                                throw new ParallelExecutionException(ex);
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

                }, GlobalExecutor.getServiceExecutor());
        return result;
    }

    private Set<Key> multicastGetAllKeysCommand(final Map<Cluster, Set<Node>> perClusterNodes, final GetKeysCommand command) throws ParallelExecutionException {
        // Parallel collection of all keys:
        Set<Key> result = ParallelUtils.parallelMap(
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
                                LOG.error(ex.getMessage(), ex);
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                                throw new ParallelExecutionException(ex);
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

                }, GlobalExecutor.getServiceExecutor());
        return result;
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
                                LOG.error(ex.getMessage(), ex);
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
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

                }, GlobalExecutor.getServiceExecutor());
        return keys;
    }

    private void handleMissingRouteException(MissingRouteException ex) throws CommunicationException {
        ErrorMessage error = ex.getErrorMessage();
        ErrorLogger.LOG(LOG, error, ex);
        throw new CommunicationException(error);
    }

    private void handleProcessingException(ProcessingException ex) throws QueryOperationException {
        ErrorMessage error = ex.getErrorMessage();
        ErrorLogger.LOG(LOG, error, ex);
        throw new QueryOperationException(error);
    }

    private void handleParallelExecutionException(ParallelExecutionException ex) throws QueryOperationException, CommunicationException {
        if (ex.getCause() instanceof ProcessingException) {
            ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex.getCause());
            throw new QueryOperationException(error);
        } else if (ex.getCause() instanceof CommunicationException) {
            throw (CommunicationException) ex.getCause();
        } else {
            throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
        }
    }

}
