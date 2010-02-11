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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import terrastore.common.ErrorMessage;
import terrastore.server.Buckets;
import terrastore.server.Parameters;
import terrastore.server.Server;
import terrastore.server.ServerOperationException;
import terrastore.server.Values;
import terrastore.service.BackupOperationException;
import terrastore.service.BackupService;
import terrastore.service.QueryOperationException;
import terrastore.service.QueryService;
import terrastore.service.UpdateOperationException;
import terrastore.service.UpdateService;
import terrastore.store.Value;
import terrastore.store.features.Predicate;
import terrastore.store.features.Update;
import terrastore.store.features.Range;

/**
 * {@link terrastore.server.Server} implementation running a json-over-http server.
 *
 * @author Sergio Bossa
 */
@Path("/")
public class JsonHttpServer implements Server {

    private static final Logger LOG = LoggerFactory.getLogger(JsonHttpServer.class);
    private final UpdateService updateService;
    private final QueryService queryService;
    private final BackupService backupService;

    public JsonHttpServer(UpdateService updateService, QueryService queryService, BackupService backupService) {
        this.updateService = updateService;
        this.queryService = queryService;
        this.backupService = backupService;
    }

    @PUT
    @Path("/{bucket}")
    @Consumes("application/json")
    public void addBucket(@PathParam("bucket") String bucket) throws ServerOperationException {
        try {
            LOG.debug("Adding bucket {}", bucket);
            updateService.addBucket(bucket);
        } catch (UpdateOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @DELETE
    @Path("/{bucket}")
    @Consumes("application/json")
    public void removeBucket(@PathParam("bucket") String bucket) throws ServerOperationException {
        try {
            LOG.debug("Removing bucket {}", bucket);
            updateService.removeBucket(bucket);
        } catch (UpdateOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @PUT
    @Path("/{bucket}/{key}")
    @Consumes("application/json")
    public void putValue(@PathParam("bucket") String bucket, @PathParam("key") String key, Value value) throws ServerOperationException {
        try {
            LOG.debug("Putting value with key {} to bucket {}", key, bucket);
            updateService.putValue(bucket, key, value);
        } catch (UpdateOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @DELETE
    @Path("/{bucket}/{key}")
    @Consumes("application/json")
    public void removeValue(@PathParam("bucket") String bucket, @PathParam("key") String key) throws ServerOperationException {
        try {
            LOG.debug("Removing value with key {} from bucket {}", key, bucket);
            updateService.removeValue(bucket, key);
        } catch (UpdateOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @POST
    @Path("/{bucket}/{key}/update")
    @Consumes("application/json")
    public void updateValue(@PathParam("bucket") String bucket, @PathParam("key") String key, @QueryParam("function") String function, @QueryParam("timeout") Long timeout, Parameters parameters) throws ServerOperationException {
        try {
            if (function == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No update function provided!");
                throw new ServerOperationException(error);
            } else if (timeout == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No update timeout provided!");
                throw new ServerOperationException(error);
            }
            LOG.debug("Updating value with key {} from bucket {}", key, bucket);
            Update update = new Update(function, timeout, parameters);
            updateService.executeUpdate(bucket, key, update);
        } catch (UpdateOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @GET
    @Path("/")
    @Produces("application/json")
    public Buckets getBuckets() throws ServerOperationException {
        try {
            LOG.debug("Getting buckets.");
            return new Buckets(queryService.getBuckets());
        } catch (QueryOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @GET
    @Path("/{bucket}/{key}")
    @Produces("application/json")
    public Value getValue(@PathParam("bucket") String bucket, @PathParam("key") String key) throws ServerOperationException {
        try {
            LOG.debug("Getting value with key {} from bucket {}", key, bucket);
            return queryService.getValue(bucket, key);
        } catch (QueryOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @GET
    @Path("/{bucket}")
    @Produces("application/json")
    public Values getAllValues(@PathParam("bucket") String bucket, @QueryParam("limit") int limit) throws ServerOperationException {
        try {
            LOG.debug("Getting all values from bucket {}", bucket);
            return new Values(queryService.getAllValues(bucket, limit));
        } catch (QueryOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @GET
    @Path("/{bucket}/range")
    @Produces("application/json")
    public Values doRangeQuery(@PathParam("bucket") String bucket, @QueryParam("startKey") String startKey, @QueryParam("endKey") String endKey, @QueryParam("limit") int limit, @QueryParam("comparator") String comparator, @QueryParam("predicate") String predicateExpression, @QueryParam("timeToLive") long timeToLive) throws ServerOperationException {
        try {
            if (startKey == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No startKey provided!");
                throw new ServerOperationException(error);
            }
            if (comparator == null) {
                comparator = "";
            }
            LOG.debug("Executing range query on bucket {}", bucket);
            Range range = new Range(startKey, endKey, limit, comparator);
            Predicate predicate = new Predicate(predicateExpression);
            return new Values(
                    queryService.doRangeQuery(bucket,
                    range,
                    predicate,
                    timeToLive));
        } catch (QueryOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @GET
    @Path("/{bucket}/predicate")
    @Produces("application/json")
    public Values doPredicateQuery(@PathParam("bucket") String bucket, @QueryParam("predicate") String predicateExpression) throws ServerOperationException {
        try {
            if (predicateExpression == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No predicate provided!");
                throw new ServerOperationException(error);
            }
            LOG.debug("Executing predicate-based query on bucket {}", bucket);
            Predicate predicate = new Predicate(predicateExpression);
            return new Values(queryService.doPredicateQuery(bucket, predicate));
        } catch (QueryOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @POST
    @Path("/{bucket}/import")
    @Consumes("application/json")
    public void doImport(@PathParam("bucket") String bucket, @QueryParam("source") String source, @QueryParam("secret") String secret) throws ServerOperationException {
        try {
            if (source == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No source provided!");
                throw new ServerOperationException(error);
            } else if (secret == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No secret provided!");
                throw new ServerOperationException(error);
            }
            LOG.debug("Importing backup for bucket {} from {}", bucket, source);
            backupService.importBackup(bucket, source, secret);
        } catch (BackupOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    @POST
    @Path("/{bucket}/export")
    @Consumes("application/json")
    public void doExport(@PathParam("bucket") String bucket, @QueryParam("destination") String destination, @QueryParam("secret") String secret) throws ServerOperationException {
        try {
            if (destination == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No destination provided!");
                throw new ServerOperationException(error);
            } else if (secret == null) {
                ErrorMessage error = new ErrorMessage(ErrorMessage.BAD_REQUEST_ERROR_CODE, "No secret provided!");
                throw new ServerOperationException(error);
            }
            LOG.debug("Exporting backup for bucket {} to {}", bucket, destination);
            backupService.exportBackup(bucket, destination, secret);
        } catch (BackupOperationException ex) {
            LOG.error(ex.getMessage(), ex);
            ErrorMessage error = ex.getErrorMessage();
            throw new ServerOperationException(error);
        }
    }

    public UpdateService getUpdateService() {
        return updateService;
    }

    public QueryService getQueryService() {
        return queryService;
    }

    @Override
    public BackupService getBackupService() {
        return backupService;
    }
}
