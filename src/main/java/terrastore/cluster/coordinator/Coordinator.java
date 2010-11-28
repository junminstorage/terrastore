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
package terrastore.cluster.coordinator;

import terrastore.cluster.ensemble.EnsembleConfiguration;
import terrastore.store.FlushCondition;
import terrastore.store.FlushStrategy;
import terrastore.communication.LocalNodeFactory;
import terrastore.communication.RemoteNodeFactory;
import terrastore.cluster.ensemble.EnsembleManager;
import terrastore.router.Router;
import terrastore.store.LockManager;
import terrastore.store.Store;

/**
 * @author Sergio Bossa
 */
public interface Coordinator {

    public void start(ServerConfiguration serverConfiguration, EnsembleConfiguration ensembleConfiguration);

    public void setCompressCommunication(boolean compressCommunication);

    public void setReconnectTimeout(long reconnectTimeout);

    public void setNodeTimeout(long nodeTimeout);

    public void setWokerThreads(int workerThreads);

    public void setLockManager(LockManager lockManager);

    public void setStore(Store store);

    public void setRouter(Router router);

    public void setEnsembleManager(EnsembleManager ensembleManager);

    public void setLocalNodeFactory(LocalNodeFactory localNodeFactory);

    public void setRemoteNodeFactory(RemoteNodeFactory remoteNodeFactory);

    public void setFlushStrategy(FlushStrategy flushStrategy);

    public void setFlushCondition(FlushCondition flushCondition);
}
