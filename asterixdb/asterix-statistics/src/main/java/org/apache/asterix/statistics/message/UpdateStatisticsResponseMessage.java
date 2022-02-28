/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.statistics.message;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICCMessageBroker.ResponseState;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INcResponse;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class UpdateStatisticsResponseMessage implements ICcAddressedMessage, INcResponse {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(UpdateStatisticsResponseMessage.class.getName());

    // statEntries -> [0] for matter synopses, [1] for antimatter synopses
    protected List<StatisticsEntry>[] statEntries;
    protected String node;
    protected String partition;
    protected ComponentStatisticsId componentId;
    protected String field;

    public UpdateStatisticsResponseMessage(List<StatisticsEntry>[] statEntries, String node, String partition,
            ComponentStatisticsId componentId, String field) {
        this.statEntries = statEntries;
        this.node = node;
        this.partition = partition;
        this.componentId = componentId;
        this.field = field;
    }

    public List<StatisticsEntry>[] getStatEntries() {
        return statEntries;
    }

    public String getNode() {
        return node;
    }

    public String getPartition() {
        return partition;
    }

    public ComponentStatisticsId getComponentId() {
        return componentId;
    }

    @Override
    public String toString() {
        return UpdateStatisticsResponseMessage.class.getSimpleName();
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        messageBroker.respond(0L, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setResult(MutablePair<ResponseState, Object> result) {
        ResponseState responseState = result.getLeft();
        switch (responseState) {
            case UNINITIALIZED:
                result.setLeft(ResponseState.SUCCESS);
                result.setRight(new ArrayList<List<StatisticsEntry>>());
                // Fall through
            case SUCCESS:
                List<List<StatisticsEntry>> combinedStats = (List<List<StatisticsEntry>>) result.getRight();
                combinedStats.add(statEntries[0]);
                combinedStats.add(statEntries[1]);
                break;
            default:
                break;
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }

    // TODO: Delete this method after testing, as it is not used anywhere else
    public void handleMessage(IMetadataProvider mdProvider) throws HyracksDataException, InterruptedException {
        boolean bActiveTxn = false;
        MetadataTransactionContext mdTxnCtx = null;
        for (int i = 0; i < 2; i++) {
            boolean isAntimatter = (i != 0);
            if (componentId.getMinTimestamp() != componentId.getMaxTimestamp()) {
                // Merged LSM component, so check whether the statistics of this component
                // is available in the Metadata cache
                for (StatisticsEntry entry : statEntries[i]) {
                    LOGGER.fine("Updating statistics of component with Id " + componentId);

                    try {
                        // drop the corresponding statistics if empty-synopsis, otherwise update
                        if (entry.getSynopsis() != null) {
//                            mdProvider.updateStatistics(entry.getDataverse(), entry.getDataset(), entry.getIndex(), node,
//                                    partition, componentId, entry.getField(), isAntimatter, entry.getSynopsis());
                        } else {
                            mdProvider.dropStatistics(entry.getDataverse(), entry.getDataset(), entry.getIndex(), node,
                                    partition, isAntimatter, entry.getField());
                        }
                    } catch (AlgebricksException | ACIDException me) {
                        if (bActiveTxn) {
                            try {
                                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                            } catch (ACIDException | RemoteException e) {
                                throw HyracksDataException.create(e);
                            }
                        }
                    }
                }
            } else {
                // LSM flushed component, so statistics will be new, add all of them (if non-empty synopsis)
                for (StatisticsEntry entry : statEntries[i]) {
                    if (entry.getSynopsis() != null) {
                        LOGGER.fine("Adding new statistics of component with Id " + componentId);

                        try {
                            mdProvider.addStatistics(entry.getDataverse(), entry.getDataset(), entry.getIndex(), node,
                                    partition, componentId, isAntimatter, entry.getField(), entry.getSynopsis());
                        } catch (AlgebricksException | ACIDException me) {
                            if (bActiveTxn) {
                                try {
                                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                                } catch (ACIDException | RemoteException e) {
                                    throw HyracksDataException.create(e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
