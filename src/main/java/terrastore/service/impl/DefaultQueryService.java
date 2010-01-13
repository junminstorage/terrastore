/**
 * Copyright 2009 Sergio Bossa (sergio.bossa@gmail.com)
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.Command;
import terrastore.communication.protocol.GetKeysCommand;
import terrastore.communication.protocol.DoRangeQueryCommand;
import terrastore.communication.protocol.GetValueCommand;
import terrastore.communication.protocol.GetValuesCommand;
import terrastore.router.Router;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.service.comparators.LexicographicalComparator;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.operators.Condition;
import terrastore.util.Maps;

/**
 * @author Sergio Bossa
 */
public class DefaultQueryService implements QueryService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueryService.class);
    private final Router router;
    private final Map<String, Comparator<String>> comparators = new HashMap<String, Comparator<String>>();
    private final Map<String, Condition> conditions = new HashMap<String, Condition>();
    private Comparator defaultComparator = new LexicographicalComparator(true);

    public DefaultQueryService(Router router) {
        this.router = router;
    }

    public Value getValue(String bucket, String key) throws QueryOperationException {
        try {
            LOG.debug("Getting value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            Command command = new GetValueCommand(bucket, key);
            Map<String, Value> entries = node.send(command);
            if (entries.size() <= 1) {
                return entries.get(key);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Illegal: more than one value found for key: " + key));
            }
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    public Map<String, Value> getAllValues(String bucket) throws QueryOperationException {
        try {
            LOG.debug("Getting all values from bucket {}", bucket);
            Set<String> storedKeys = getAllKeysForBucket(bucket);
            Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, storedKeys);
            List<Map<String, Value>> allKeyValues = new ArrayList(nodeToKeys.size());
            for (Map.Entry<Node, Set<String>> nodeToKeysEntry : nodeToKeys.entrySet()) {
                Node node = nodeToKeysEntry.getKey();
                Set<String> keys = nodeToKeysEntry.getValue();
                Command command = new GetValuesCommand(bucket, keys);
                allKeyValues.add(node.send(command));
            }
            return Maps.union(allKeyValues);
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<String, Value> doRangeQuery(String bucket, Range range, Predicate predicate, long timeToLive) throws QueryOperationException {
        try {
            LOG.debug("Range query on bucket {}", bucket);
            Comparator<String> keyComparator = getComparator(range.getKeyComparatorName());
            Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            Set<String> storedKeys = getKeyRangeForBucket(bucket, range, keyComparator, timeToLive);
            Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, storedKeys);
            List<Map<String, Value>> allKeyValues = new ArrayList(nodeToKeys.size());
            for (Map.Entry<Node, Set<String>> nodeToKeysEntry : nodeToKeys.entrySet()) {
                Node node = nodeToKeysEntry.getKey();
                Set<String> keys = nodeToKeysEntry.getValue();
                Command command = null;
                if (valueCondition == null) {
                    command = new GetValuesCommand(bucket, keys);
                } else {
                    command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                }
                allKeyValues.add(node.send(command));
            }
            // TODO: we may use fork/join to build the final map out of all sub-maps.
            return Maps.drain(allKeyValues, new TreeMap<String, Value>(keyComparator));
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    @Override
    public Map<String, Value> doPredicateQuery(String bucket, Predicate predicate) throws QueryOperationException {
        try {
            LOG.debug("Predicate-based query on bucket {}", bucket);
            Condition valueCondition = predicate.isEmpty() ? null : getCondition(predicate.getConditionType());
            if (valueCondition != null) {
                Set<String> storedKeys = getAllKeysForBucket(bucket);
                Map<Node, Set<String>> nodeToKeys = router.routeToNodesFor(bucket, storedKeys);
                List<Map<String, Value>> allKeyValues = new ArrayList(nodeToKeys.size());
                for (Map.Entry<Node, Set<String>> nodeToKeysEntry : nodeToKeys.entrySet()) {
                    Node node = nodeToKeysEntry.getKey();
                    Set<String> keys = nodeToKeysEntry.getValue();
                    Command command = new GetValuesCommand(bucket, keys, predicate, valueCondition);
                    allKeyValues.add(node.send(command));
                }
                return Maps.union(allKeyValues);
            } else {
                throw new QueryOperationException(new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "Wrong predicate!"));
            }
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new QueryOperationException(error);
        }
    }

    public Comparator<String> getDefaultComparator() {
        return defaultComparator;
    }

    public Map<String, Comparator<String>> getComparators() {
        return comparators;
    }

    @Override
    public Map<String, Condition> getConditions() {
        return conditions;
    }

    public Router getRouter() {
        return router;
    }

    public void setDefaultComparator(Comparator defaultComparator) {
        this.defaultComparator = defaultComparator;
    }

    public void setComparators(Map<String, Comparator<String>> comparators) {
        this.comparators.clear();
        this.comparators.putAll(comparators);
    }

    public void setConditions(Map<String, Condition> conditions) {
        this.conditions.clear();
        this.conditions.putAll(conditions);
    }

    private Set<String> getAllKeysForBucket(String bucket) throws ProcessingException {
        Node node = router.getLocalNode();
        Command command = new GetKeysCommand(bucket);
        Set<String> storedKeys = node.send(command).keySet();
        return storedKeys;
    }

    private Set<String> getKeyRangeForBucket(String bucket, Range keyRange, Comparator<String> keyComparator, long timeToLive) throws ProcessingException {
        Node node = router.getLocalNode();
        Command command = new DoRangeQueryCommand(bucket, keyRange, keyComparator, timeToLive);
        Set<String> storedKeys = node.send(command).keySet();
        return storedKeys;
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
