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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        LOG.debug("Getting bucket names.");
        GetBucketsCommand command = new GetBucketsCommand();
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<String> buckets = multicastGetBucketsCommand(perClusterNodes, command);
        return buckets;
    }

    @Override
    public Value getValue(String bucket, String key, Predicate predicate) throws QueryOperationException {
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
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<String, Value> getAllValues(final String bucket, final int limit) throws QueryOperationException {
        try {
            LOG.debug("Getting all values from bucket {}", bucket);
            Set<String> allKeys = Sets.limited(getAllKeysForBucket(bucket), limit);
            Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, allKeys);
            List<Map<String, Value>> allKeyValues = ParallelUtils.<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>parallelMap(
                    new ArrayList<Map.Entry<Node, Set<String>>>(nodeToKeys.entrySet()),
                    new MapCollector<Map<String, Value>, List<Map<String, Value>>>() {

                        @Override
                        public List<Map<String, Value>> createCollector() {
                            return new LinkedList<Map<String, Value>>();
                        }
                    },
                    new MapTask<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>() {

                        @Override
                        public List<Map<String, Value>> map(Map.Entry<Node, Set<String>> nodeToKeys, MapCollector<Map<String, Value>, List<Map<String, Value>>> collector) {
                            try {
                                List<Map<String, Value>> result = collector.createCollector();
                                Node node = nodeToKeys.getKey();
                                Set<String> keys = nodeToKeys.getValue();
                                GetValuesCommand command = new GetValuesCommand(bucket, keys);
                                result.add(node.<Map<String, Value>>send(command));
                                return result;
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                                ErrorMessage error = ex.getErrorMessage();
                                return null;
                                //throw new QueryOperationException(error);
                            }
                        }
                    });
            return Maps.union(allKeyValues);
        } catch (MissingRouteException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<String, Value> queryByRange(final String bucket, final Range range, final Predicate predicate, final long timeToLive) throws QueryOperationException {
        try {
            LOG.debug("Range query on bucket {}", bucket);
            final Comparator keyComparator = getComparator(range.getKeyComparatorName());
            final Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            Set<String> keysInRange = Sets.limited(getKeyRangeForBucket(bucket, range, keyComparator, timeToLive), range.getLimit());
            Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, keysInRange);
            List<Map<String, Value>> allKeyValues = ParallelUtils.<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>parallelMap(
                    new ArrayList<Map.Entry<Node, Set<String>>>(nodeToKeys.entrySet()),
                    new MapCollector<Map<String, Value>, List<Map<String, Value>>>() {

                        @Override
                        public List<Map<String, Value>> createCollector() {
                            return new LinkedList<Map<String, Value>>();
                        }
                    },
                    new MapTask<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>() {

                        @Override
                        public List<Map<String, Value>> map(Map.Entry<Node, Set<String>> nodeToKeys, MapCollector<Map<String, Value>, List<Map<String, Value>>> collector) {
                            try {
                                List<Map<String, Value>> result = collector.createCollector();
                                Node node = nodeToKeys.getKey();
                                Set<String> keys = nodeToKeys.getValue();
                                GetValuesCommand command = null;
                                if (valueCondition == null) {
                                    command = new GetValuesCommand(bucket, keys);
                                } else {
                                    command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                                }
                                result.add(node.<Map<String, Value>>send(command));
                                return result;
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                                ErrorMessage error = ex.getErrorMessage();
                                return null;
                                //throw new QueryOperationException(error);
                            }
                        }
                    });
            return Maps.composite(keysInRange, allKeyValues);
        } catch (MissingRouteException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<String, Value> queryByPredicate(final String bucket, final Predicate predicate) throws QueryOperationException {
        try {
            LOG.debug("Predicate-based query on bucket {}", bucket);
            final Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            if (valueCondition != null) {
                Set<String> allKeys = getAllKeysForBucket(bucket);
                Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, allKeys);
                List<Map<String, Value>> allKeyValues = ParallelUtils.<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>parallelMap(
                        new ArrayList<Map.Entry<Node, Set<String>>>(nodeToKeys.entrySet()),
                        new MapCollector<Map<String, Value>, List<Map<String, Value>>>() {

                            @Override
                            public List<Map<String, Value>> createCollector() {
                                return new LinkedList<Map<String, Value>>();
                            }
                        },
                        new MapTask<Map.Entry<Node, Set<String>>, Map<String, Value>, List<Map<String, Value>>>() {

                            @Override
                            public List<Map<String, Value>> map(Map.Entry<Node, Set<String>> nodeToKeys, MapCollector<Map<String, Value>, List<Map<String, Value>>> collector) {
                                try {
                                    List<Map<String, Value>> result = collector.createCollector();
                                    Node node = nodeToKeys.getKey();
                                    Set<String> keys = nodeToKeys.getValue();
                                    GetValuesCommand command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                                    result.add(node.<Map<String, Value>>send(command));
                                    return result;
                                } catch (ProcessingException ex) {
                                    LOG.error(ex.getMessage(), ex);
                                    ErrorMessage error = ex.getErrorMessage();
                                    return null;
                                    //throw new QueryOperationException(error);
                                }
                            }
                        });
                return Maps.union(allKeyValues);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong predicate!"));
            }
        } catch (MissingRouteException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
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

    private Set<String> getAllKeysForBucket(String bucket) throws QueryOperationException {
        GetKeysCommand command = new GetKeysCommand(bucket);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<String> keys = multicastGetAllKeysCommand(perClusterNodes, command);
        return keys;
    }

    private Set<String> getKeyRangeForBucket(String bucket, Range keyRange, Comparator keyComparator, long timeToLive) throws QueryOperationException {
        RangeQueryCommand command = new RangeQueryCommand(bucket, keyRange, keyComparator, timeToLive);
        Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
        Set<String> keys = multicastRangeQueryCommand(perClusterNodes, command);
        return keys;
    }

    private Set<String> multicastGetBucketsCommand(final Map<Cluster, Set<Node>> perClusterNodes, final GetBucketsCommand command) throws QueryOperationException {
        // Parallel collection of all buckets:
        Set<String> result = ParallelUtils.<Set<Node>, String, Set<String>>parallelMap(
                new ArrayList<Set<Node>>(perClusterNodes.values()),
                new MapCollector<String, Set<String>>() {

                    @Override
                    public Set<String> createCollector() {
                        return new HashSet<String>();
                    }
                },
                new MapTask<Set<Node>, String, Set<String>>() {

                    @Override
                    public Set<String> map(Set<Node> nodes, MapCollector<String, Set<String>> collector) {
                        Set<String> buckets = collector.createCollector();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                Set<String> partial = node.<Set<String>>send(command);
                                buckets.addAll(partial);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return buckets;
                    }
                });
        return result;
    }

    private Set<String> multicastGetAllKeysCommand(final Map<Cluster, Set<Node>> perClusterNodes, final GetKeysCommand command) throws QueryOperationException {
        // Parallel collection of all keys:
        Set<String> result = ParallelUtils.<Set<Node>, String, Set<String>>parallelMap(
                new ArrayList<Set<Node>>(perClusterNodes.values()),
                new MapCollector<String, Set<String>>() {

                    @Override
                    public Set<String> createCollector() {
                        return new HashSet<String>();
                    }
                },
                new MapTask<Set<Node>, String, Set<String>>() {

                    @Override
                    public Set<String> map(Set<Node> nodes, MapCollector<String, Set<String>> collector) {
                        Set<String> keys = collector.createCollector();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                keys = node.<Set<String>>send(command);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return keys;
                    }
                });
        return result;
    }

    private Set<String> multicastRangeQueryCommand(final Map<Cluster, Set<Node>> perClusterNodes, final RangeQueryCommand command) throws QueryOperationException {
        // Parallel collection of all sets of sorted keys in a list:
        List<Set<String>> keys = ParallelUtils.<Set<Node>, Set<String>, List<Set<String>>>parallelMap(
                new ArrayList<Set<Node>>(perClusterNodes.values()),
                new MapCollector<Set<String>, List<Set<String>>>() {

                    @Override
                    public List<Set<String>> createCollector() {
                        return new LinkedList<Set<String>>();
                    }
                },
                new MapTask<Set<Node>, Set<String>, List<Set<String>>>() {

                    @Override
                    public List<Set<String>> map(Set<Node> nodes, MapCollector<Set<String>, List<Set<String>>> collector) {
                        List<Set<String>> keys = collector.createCollector();
                        // Try to send command, stopping after first successful attempt:
                        for (Node node : nodes) {
                            try {
                                Set<String> partial = node.<Set<String>>send(command);
                                keys.add(partial);
                                // Break after first success, we just want to send command to one node per cluster:
                                break;
                            } catch (ProcessingException ex) {
                                LOG.error(ex.getMessage(), ex);
                            }
                        }
                        return keys;
                    }
                });
        // Parallel merge of all sorted sets:
        return ParallelUtils.parallelMerge(keys);
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
