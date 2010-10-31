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
import terrastore.communication.protocol.PutValueCommand;
import terrastore.communication.protocol.RemoveBucketCommand;
import terrastore.communication.protocol.RemoveValueCommand;
import terrastore.communication.protocol.UpdateCommand;
import terrastore.router.MissingRouteException;
import terrastore.router.Router;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.util.json.JsonUtils;
import terrastore.store.ValidationException;

/**
 * @author Sergio Bossa
 */
public class DefaultUpdateService implements UpdateService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultUpdateService.class);
    //
    private final Router router;

    public DefaultUpdateService(Router router) {
        this.router = router;
    }

    public void removeBucket(String bucket) throws CommunicationException, UpdateOperationException {
        try {
            LOG.debug("Removing bucket {}", bucket);
            RemoveBucketCommand command = new RemoveBucketCommand(bucket);
            Map<Cluster, Set<Node>> perClusterNodes = router.broadcastRoute();
            multicastRemoveBucketCommand(perClusterNodes, command);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        }
    }

    public void putValue(String bucket, Key key, Value value, Predicate predicate) throws CommunicationException, UpdateOperationException, ValidationException {
        try {
            LOG.debug("Putting value with key {} to bucket {}", key, bucket);
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
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ProcessingException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new UpdateOperationException(error);
        }
    }

    public void removeValue(String bucket, Key key) throws CommunicationException, UpdateOperationException {
        try {
            LOG.debug("Removing value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            RemoveValueCommand command = new RemoveValueCommand(bucket, key);
            node.send(command);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ProcessingException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new UpdateOperationException(error);
        }
    }

    @Override
    public Value updateValue(String bucket, Key key, Update update) throws CommunicationException, UpdateOperationException {
        try {
            LOG.debug("Updating value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            UpdateCommand command = new UpdateCommand(bucket, key, update);
            return node.<Value>send(command);
        } catch (MissingRouteException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new CommunicationException(error);
        } catch (ProcessingException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new UpdateOperationException(error);
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

}
