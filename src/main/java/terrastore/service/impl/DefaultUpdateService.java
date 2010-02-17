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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.communication.Node;
import terrastore.communication.ProcessingException;
import terrastore.communication.protocol.AddBucketCommand;
import terrastore.communication.protocol.PutValueCommand;
import terrastore.communication.protocol.RemoveBucketCommand;
import terrastore.communication.protocol.RemoveValueCommand;
import terrastore.communication.protocol.UpdateCommand;
import terrastore.event.EventBus;
import terrastore.event.ValueChangedEvent;
import terrastore.event.ValueRemovedEvent;
import terrastore.router.Router;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.features.Update;
import terrastore.store.Value;
import terrastore.store.operators.Function;

/**
 * @author Sergio Bossa
 */
public class DefaultUpdateService implements UpdateService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultUpdateService.class);
    private Map<String, Function> functions = new HashMap<String, Function>();
    private final Router router;
    private final EventBus eventBus;

    public DefaultUpdateService(Router router, EventBus eventBus) {
        this.router = router;
        this.eventBus = eventBus;
    }

    public void addBucket(String bucket) throws UpdateOperationException {
        try {
            LOG.debug("Adding bucket {}", bucket);
            Node node = router.getLocalNode();
            AddBucketCommand command = new AddBucketCommand(bucket);
            node.send(command);
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new UpdateOperationException(error);
        }
    }

    public void removeBucket(String bucket) throws UpdateOperationException {
        try {
            LOG.debug("Removing bucket {}", bucket);
            Node node = router.getLocalNode();
            RemoveBucketCommand command = new RemoveBucketCommand(bucket);
            node.send(command);
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new UpdateOperationException(error);
        }
    }

    public void putValue(String bucket, String key, Value value) throws UpdateOperationException {
        try {
            LOG.debug("Putting value with key {} to bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            PutValueCommand command = new PutValueCommand(bucket, key, value);
            node.send(command);
            eventBus.publish(new ValueChangedEvent(bucket, key, value.getBytes()));
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new UpdateOperationException(error);
        }
    }

    public void removeValue(String bucket, String key) throws UpdateOperationException {
        try {
            LOG.debug("Removing value with key {} from bucket {}", key, bucket);
            Node node = router.routeToNodeFor(bucket, key);
            RemoveValueCommand command = new RemoveValueCommand(bucket, key);
            node.send(command);
            eventBus.publish(new ValueRemovedEvent(bucket, key));
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new UpdateOperationException(error);
        }
    }

    @Override
    public void executeUpdate(String bucket, String key, Update update) throws UpdateOperationException {
        try {
            LOG.debug("Updating value with key {} from bucket {}", key, bucket);
            Function function = functions.get(update.getFunctionName());
            if (function != null) {
                Node node = router.routeToNodeFor(bucket, key);
                UpdateCommand command = new UpdateCommand(bucket, key, update, function);
                Value updated = node.<Value>send(command);
                if (updated != null) {
                    eventBus.publish(new ValueChangedEvent(bucket, key, updated.getBytes()));
                }
            } else {
                throw new UpdateOperationException(new ErrorMessage(ErrorMessage.INTERNAL_SERVER_ERROR_CODE, "No function found: " + update.getFunctionName()));
            }
        } catch (ProcessingException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new UpdateOperationException(error);
        }
    }

    @Override
    public Map<String, Function> getFunctions() {
        return functions;
    }

    @Override
    public Router getRouter() {
        return router;
    }

    @Override
    public EventBus getEventBus() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setFunctions(Map<String, Function> functions) {
        this.functions = functions;
    }
}
