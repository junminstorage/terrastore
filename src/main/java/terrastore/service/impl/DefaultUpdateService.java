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
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorLogger;
import terrastore.common.ErrorMessage;
import terrastore.communication.Cluster;
import terrastore.communication.CommunicationException;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.PutValueCommand;
import terrastore.communication.protocol.RemoveBucketCommand;
import terrastore.communication.protocol.RemoveValueCommand;
import terrastore.communication.protocol.RemoveValuesCommand;
import terrastore.communication.protocol.UpdateCommand;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.server.Keys;
import terrastore.service.KeyRangeService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.features.Range;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.util.collect.Sets;
import terrastore.util.collect.parallel.MapCollector;
import terrastore.util.collect.parallel.MapTask;
import terrastore.util.collect.parallel.ParallelExecutionException;
import terrastore.util.collect.parallel.ParallelUtils;
import terrastore.util.concurrent.GlobalExecutor;
import terrastore.util.json.JsonUtils;
import terrastore.store.ValidationException;

/**
 * @author Sergio Bossa
 */
public class DefaultUpdateService implements UpdateService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultUpdateService.class);
    //
    private final Router router;
    private final KeyRangeService keyRangeService;

    public DefaultUpdateService(Router router, KeyRangeService keyRangeService) {
        this.router = router;
        this.keyRangeService = keyRangeService;
    }

    public void removeBucket(String bucket) throws CommunicationException, UpdateOperationException {
        try {
            RemoveBucketCommand command = new RemoveBucketCommand(bucket);
            Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
            multicastRemoveBucketCommand(perClusterNodes, command);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
        }
    }

    public void putValue(String bucket, Key key, Value value, Predicate predicate) throws CommunicationException, UpdateOperationException, ValidationException {
        try {
            JsonUtils.validate(value);
            Node node = router.routeToNodeFor(bucket, key);
            PutValueCommand command = null;
            if (predicate == null || predicate.isEmpty()) {
                command = new PutValueCommand(bucket, key, value);
            } else {
                command = new PutValueCommand(bucket, key, value, predicate);
            }
            node.send(command);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
        } catch (ProcessingException ex) {
            handleProcessingException(ex);
        }
    }

    public void removeValue(String bucket, Key key) throws CommunicationException, UpdateOperationException {
        try {
            Node node = router.routeToNodeFor(bucket, key);
            RemoveValueCommand command = new RemoveValueCommand(bucket, key);
            node.send(command);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
        } catch (ProcessingException ex) {
            handleProcessingException(ex);
        }
    }

    @Override
    public Value updateValue(String bucket, Key key, Update update) throws CommunicationException, UpdateOperationException {
        try {
            Node node = router.routeToNodeFor(bucket, key);
            UpdateCommand command = new UpdateCommand(bucket, key, update);
            return node.<Value>send(command);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ProcessingException ex) {
            handleProcessingException(ex);
            return null;
        }
    }

    @Override
    public Keys removeByRange(final String bucket, Range range, final Predicate predicate) throws CommunicationException, UpdateOperationException {
        try {
            Set<Key> keysInRange = Sets.limited(keyRangeService.getKeyRangeForBucket(bucket, range), range.getLimit());
            Map<Node, Set<Key>> nodeToKeys = router.routeToNodesFor(bucket, keysInRange);
            List<Set<Key>> removedKeyMap = ParallelUtils.parallelMap(
                    nodeToKeys.entrySet(),
                    new MapTask<Map.Entry<Node, Set<Key>>, Set<Key>>() {

                        @Override
                        public Set<Key> map(Entry<Node, Set<Key>> nodeToKeys) throws ParallelExecutionException {
                            try {
                                Node node = nodeToKeys.getKey();
                                Set<Key> keys = nodeToKeys.getValue();
                                RemoveValuesCommand command = null;
                                if (predicate.isEmpty()) {
                                    command = new RemoveValuesCommand(bucket, keys);
                                } else {
                                    command = new RemoveValuesCommand(bucket, keys, predicate);
                                }
                                return node.<Set<Key>>send(command);
                            } catch (Exception ex) {
                                throw new ParallelExecutionException(ex);
                            }
                        }

                    },
                    new MapCollector<Set<Key>, List<Set<Key>>>() {

                        @Override
                        public List<Set<Key>> collect(List<Set<Key>> allKeyValues) {
                            return allKeyValues;
                        }

                    },
                    GlobalExecutor.getUpdateExecutor());

            Set<Key> removedKeys = new HashSet<Key>();
            for (Set<Key> keysFromNode : removedKeyMap) {
                removedKeys.addAll(keysFromNode);
            }

            return new Keys(removedKeys);
        } catch (MissingRouteException ex) {
            handleMissingRouteException(ex);
            return null;
        } catch (ParallelExecutionException ex) {
            handleParallelExecutionException(ex);
            return null;
        }
    }

    @Override
    public Router getRouter() {
        return router;
    }

    private void multicastRemoveBucketCommand(Map<Cluster, Set<Node>> perClusterNodes, RemoveBucketCommand command) throws MissingRouteException, UpdateOperationException {
        for (Set<Node> nodes : perClusterNodes.values()) {
            boolean successful = true;
            // There must be connected cluster nodes, else throw MissingRouteException:
            if (!nodes.isEmpty()) {
                // Try to send command, stopping after first successful attempt:
                for (Node node : nodes) {
                    try {
                        node.send(command);
                        // Break after first success, we just want to send command to one node per cluster:
                        successful = true;
                        break;
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage(), ex);
                        successful = false;
                    }
                }
                // If all nodes failed, throw exception:
                if (!successful) {
                    throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "The operation has been only partially applied. Some clusters of your ensemble may be down or unreachable."));
                }
            } else {
                throw new MissingRouteException(new ErrorMessage(ErrorMessage.UNAVAILABLE_ERROR_CODE, "The operation has been only partially applied. Some clusters of your ensemble may be down or unreachable."));
            }
        }
    }

    private void handleMissingRouteException(MissingRouteException ex) throws CommunicationException {
        ErrorMessage error = ex.getErrorMessage();
        ErrorLogger.LOG(LOG, error, ex);
        throw new CommunicationException(error);
    }

    private void handleProcessingException(ProcessingException ex) throws UpdateOperationException {
        ErrorMessage error = ex.getErrorMessage();
        ErrorLogger.LOG(LOG, error, ex);
        throw new UpdateOperationException(error);
    }

    private void handleParallelExecutionException(ParallelExecutionException ex) throws UpdateOperationException, CommunicationException {
        if (ex.getCause() instanceof ProcessingException) {
            ErrorMessage error = ((ProcessingException) ex.getCause()).getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex.getCause());
            throw new UpdateOperationException(error);
        } else if (ex.getCause() instanceof CommunicationException) {
            throw (CommunicationException) ex.getCause();
        } else {
            throw new UpdateOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "Unexpected error: " + ex.getMessage()));
        }
    }

}
