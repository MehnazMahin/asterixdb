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

public class UpdateStatisticsResponseMessage implements ICcAddressedMessage, INcResponse {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(UpdateStatisticsResponseMessage.class.getName());

    // statEntries -> [0] for non-antimatter synopses, [1] for antimatter synopses
    protected StatisticsEntry[] statEntries;
    protected String field;

    public UpdateStatisticsResponseMessage(StatisticsEntry[] statEntries, String field) {
        this.statEntries = statEntries;
        this.field = field;
    }

    public StatisticsEntry[] getStatEntries() {
        return statEntries;
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
                result.setRight(new ArrayList<StatisticsEntry>());
                // Fall through
            case SUCCESS:
                List<StatisticsEntry> combinedStats = (List<StatisticsEntry>) result.getRight();
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
            // Merged LSM component, so check whether the statistics of this component
            // is available in the Metadata cache
            StatisticsEntry entry = statEntries[i];
            if (entry != null) {
                try {
                    // drop the corresponding statistics if empty-synopsis, otherwise update
                    if (entry.getSynopsis() != null) {
                        //                            mdProvider.updateStatistics(entry.getDataverse(), entry.getDataset(), entry.getIndex(), node,
                        //                                    partition, componentId, entry.getField(), isAntimatter, entry.getSynopsis());
                    } else {
                        mdProvider.dropStatistics("", "", "", isAntimatter, entry.getField());
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
        }
    }
}