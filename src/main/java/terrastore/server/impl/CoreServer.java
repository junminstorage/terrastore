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
package terrastore.server.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ClusterStats;
import terrastore.common.ErrorLogger;
import terrastore.common.ErrorMessage;
import terrastore.communication.CommunicationException;
import terrastore.server.Buckets;
import terrastore.server.MapReduceDescriptor;
import terrastore.server.Parameters;
import terrastore.server.Server;
import terrastore.server.ServerOperationException;
import terrastore.server.Values;
import terrastore.service.BackupOperationException;
import terrastore.service.BackupService;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.service.StatsService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.features.Range;
import terrastore.store.features.Reducer;

/**
 * Core {@link terrastore.server.Server} implementation.
 *
 * @author Sergio Bossa
 */
public class CoreServer implements Server {

    private static final Logger LOG = LoggerFactory.getLogger(CoreServer.class);
    //
    private final UpdateService updateService;
    private final QueryService queryService;
    private final BackupService backupService;
    private final StatsService statsService;

    public CoreServer(UpdateService updateService, QueryService queryService, BackupService backupService, StatsService statsService) {
        this.updateService = updateService;
        this.queryService = queryService;
        this.backupService = backupService;
        this.statsService = statsService;
    }

    public void removeBucket(String bucket) throws ServerOperationException {
        try {
            LOG.info("Removing bucket {}", bucket);
            updateService.removeBucket(bucket);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (UpdateOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public void putValue(String bucket, Key key, Value value, String predicate) throws ServerOperationException {
        try {
            LOG.info("Putting value with key {} to bucket {}", key, bucket);
            updateService.putValue(bucket, key, value, new Predicate(predicate));
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (UpdateOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public void removeValue(String bucket, Key key) throws ServerOperationException {
        try {
            LOG.info("Removing value with key {} from bucket {}", key, bucket);
            updateService.removeValue(bucket, key);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (UpdateOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Value updateValue(String bucket, Key key, String function, Long timeout, Parameters parameters) throws ServerOperationException {
        try {
            if (function == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No update function provided!");
                throw new ServerOperationException(error);
            } else if (timeout == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No update timeout provided!");
                throw new ServerOperationException(error);
            }
            LOG.info("Updating value with key {} and function {} from bucket {}", new Object[]{key, function, bucket});
            Update update = new Update(function, timeout, parameters);
            return updateService.updateValue(bucket, key, update);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (UpdateOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Buckets getBuckets() throws ServerOperationException {
        try {
            LOG.info("Getting buckets.");
            return new Buckets(queryService.getBuckets());
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Value getValue(String bucket, Key key, String predicate) throws ServerOperationException {
        try {
            LOG.info("Getting value with key {} from bucket {}", key, bucket);
            return queryService.getValue(bucket, key, new Predicate(predicate));
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Values getAllValues(String bucket, int limit) throws ServerOperationException {
        try {
            LOG.info("Getting all values from bucket {}", bucket);
            return new Values(queryService.getAllValues(bucket, limit));
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Values queryByRange(String bucket, Key startKey, Key endKey, int limit, String comparator, String predicateExpression, long timeToLive) throws ServerOperationException {
        try {
            if (startKey == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No startKey provided!");
                throw new ServerOperationException(error);
            }
            if (comparator == null) {
                comparator = "";
            }
            LOG.info("Executing range query from {} to {} ordered by {} on bucket {}", new Object[]{startKey, endKey, comparator, bucket});
            Range range = new Range(startKey, endKey, limit, comparator, timeToLive);
            Predicate predicate = new Predicate(predicateExpression);
            return new Values(
                    queryService.queryByRange(bucket,
                    range,
                    predicate));
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public Values queryByPredicate(String bucket, String predicateExpression) throws ServerOperationException {
        try {
            if (predicateExpression == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No predicate provided!");
                throw new ServerOperationException(error);
            }
            LOG.info("Executing predicate query {} on bucket {}", predicateExpression, bucket);
            Predicate predicate = new Predicate(predicateExpression);
            return new Values(queryService.queryByPredicate(bucket, predicate));
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    @Override
    public Value queryByMapReduce(String bucket, MapReduceDescriptor descriptor) throws ServerOperationException {
        try {
            descriptor.sanitize();
            LOG.info("Executing map reduce query on bucket {}", bucket);
            Range range = new Range(descriptor.range.startKey, descriptor.range.endKey, 0, descriptor.range.comparator, descriptor.range.timeToLive);
            Mapper mapper = new Mapper(descriptor.task.mapper, descriptor.task.combiner, descriptor.task.timeout, descriptor.task.parameters);
            Reducer reducer = new Reducer(descriptor.task.reducer, descriptor.task.timeout);
            return queryService.queryByMapReduce(bucket, range, mapper, reducer);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (QueryOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public void importBackup(String bucket, String source, String secret) throws ServerOperationException {
        try {
            if (source == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No source provided!");
                throw new ServerOperationException(error);
            } else if (secret == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No secret provided!");
                throw new ServerOperationException(error);
            }
            LOG.info("Importing backup for bucket {} from {}", bucket, source);
            backupService.importBackup(bucket, source, secret);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (BackupOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public void exportBackup(String bucket, String destination, String secret) throws ServerOperationException {
        try {
            if (destination == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No destination provided!");
                throw new ServerOperationException(error);
            } else if (secret == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No secret provided!");
                throw new ServerOperationException(error);
            }
            LOG.info("Exporting backup for bucket {} to {}", bucket, destination);
            backupService.exportBackup(bucket, destination, secret);
        } catch (CommunicationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        } catch (BackupOperationException ex) {
            ErrorMessage error = ex.getErrorMessage();
            ErrorLogger.LOG(LOG, error, ex);
            throw new ServerOperationException(error);
        }
    }

    public ClusterStats getClusterStats() {
        LOG.info("Getting cluster statistics.");
        return statsService.getClusterStats();
    }

    public UpdateService getUpdateService() {
        return updateService;
    }

    public QueryService getQueryService() {
        return queryService;
    }

    public BackupService getBackupService() {
        return backupService;
    }

}
