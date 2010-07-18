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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorLogger;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
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
import terrastore.service.comparators.LexicographicalComparator;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.operators.Comparator;
import terrastore.store.operators.Condition;
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
    private final Map<String, Comparator> comparators = new HashMap<String, Comparator>();
    private final Map<String, Condition> conditions = new HashMap<String, Condition>();
    private Comparator defaultComparator = new LexicographicalComparator(true);

    public DefaultQueryService(Router router) {
        this.router = router;
    }

    @Override
    public Set<String> getBuckets() throws QueryOperationException {
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
    public Value getValue(String bucket, Key key, Predicate predicate) throws QueryOperationException {
        try {
            LOG.debug("Getting value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            GetValueCommand command = null;
            if (predicate == null || predicate.isEmpty()) {
                command = new GetValueCommand(bucket, key);
            } else {
                Condition condition = getCondition(predicate.getConditionType());
                command = new GetValueCommand(bucket, key, predicate, condition);
            }
            Value result = node.<Value>send(command);
            return result;
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new QueryOperationException(error);
        } catch (ProcessingException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<Key, Value> getAllValues(final String bucket, final int limit) throws QueryOperationException {
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
                            } catch (ProcessingException ex) {
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
            throw new QueryOperationException(error);
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
    public Map<Key, Value> queryByRange(final String bucket, final Range range, final Predicate predicate, final long timeToLive) throws QueryOperationException {
        try {
            LOG.debug("Range query on bucket {}", bucket);
            final Comparator keyComparator = getComparator(range.getKeyComparatorName());
            final Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            Set<Key> keysInRange = Sets.limited(getKeyRangeForBucket(bucket, range, keyComparator, timeToLive), range.getLimit());
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
                                if (valueCondition == null) {
                                    command = new GetValuesCommand(bucket, keys);
                                } else {
                                    command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                                }
                                return node.<Map<Key, Value>>send(command);
                            } catch (ProcessingException ex) {
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
            throw new QueryOperationException(error);
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
    public Map<Key, Value> queryByPredicate(final String bucket, final Predicate predicate) throws QueryOperationException {
        try {
            LOG.debug("Predicate-based query on bucket {}", bucket);
            final Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            if (valueCondition != null) {
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
                                    GetValuesCommand command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                                    return node.<Map<Key, Value>>send(command);
                                } catch (ProcessingException ex) {
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
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong predicate!"));
            }
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new QueryOperationException(error);
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
    public Comparator getDefaultComparator() {
        return defaultComparator;
    }

    @Override
    public Map<String, Comparator> getComparators() {
        return comparators;
    }

    @Override
    public Map<String, Condition> getConditions() {
        return conditions;
    }

    @Override
    public Router getRouter() {
        return router;
    }

    public void setDefaultComparator(Comparator defaultComparator) {
        this.defaultComparator = defaultComparator;
    }

    public void setComparators(Map<String, Comparator> comparators) {
        this.comparators.clear();
        this.comparators.putAll(comparators);
    }

    public void setConditions(Map<String, Condition> conditions) {
        this.conditions.clear();
        this.conditions.putAll(conditions);
    }

    private Set<Key> getAllKeysForBucket(String bucket) throws ParallelExecutionException {
        GetKeysCommand command = new GetKeysCommand(bucket);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<Key> keys = multicastGetAllKeysCommand(perClusterNodes, command);
        return keys;
    }

    private Set<Key> getKeyRangeForBucket(String bucket, Range keyRange, Comparator keyComparator, long timeToLive) throws ParallelExecutionException {
        RangeQueryCommand command = new RangeQueryCommand(bucket, keyRange, keyComparator, timeToLive);
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
                            } catch (ProcessingException ex) {
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
                            } catch (ProcessingException ex) {
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
                            } catch (ProcessingException ex) {
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

    private Comparator getComparator(String comparatorName) {
        if (comparators.containsKey(comparatorName)) {
            return comparators.get(comparatorName);
        }
        return defaultComparator;
    }

    private Condition getCondition(String conditionType) throws QueryOperationException {
        if (conditions.containsKey(conditionType)) {
            return conditions.get(conditionType);
        } else {
            throw new QueryOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong condition type: " + conditionType));
        }
    }
}
