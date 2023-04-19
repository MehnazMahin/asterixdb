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
package org.apache.asterix.statistics.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.asterix.metadata.utils.ISynopsisCombinationsHelper.NonEquiHeightSynopsisElement;
import org.apache.asterix.metadata.utils.SynopsisUtils;
import org.apache.asterix.statistics.IndexListStatistics;
import org.apache.asterix.statistics.message.UpdateStatisticsResponseMessage;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IStatisticsManager;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisElementType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;

public class StatisticsManager implements IStatisticsManager {

    private final INCServiceContext ncContext;
    private final Map<ILSMDiskComponent, StatisticsEntry> synopsisMap;
    private final Map<ILSMDiskComponent, StatisticsEntry> antimatterSynopsisMap;

    private final IValueReference MATTER_STATISTICS_FLAG = BooleanPointable.FACTORY.createPointable(false);
    private final IValueReference ANTIMATTER_STATISTICS_FLAG = BooleanPointable.FACTORY.createPointable(true);

    public StatisticsManager(INCServiceContext ncApplicationContext) {
        ncContext = ncApplicationContext;
        synopsisMap = new HashMap<>();
        antimatterSynopsisMap = new HashMap<>();
    }

    private List<String> parsePathComponents(String componentPath) throws HyracksDataException {
        //TODO: Find a more elegant way of getting dataverse/dataset/timestamp from stats rather then parsing filepaths
        String numPattern = "\\d";
        String namePattern = "([^" + File.separator + "]+)";
        String dirPattern = namePattern + File.separator;
        String indexDatasetPattern =
                namePattern + File.separator + numPattern + File.separator + namePattern + File.separator;
        // Disk component name format: T2_T1_s. T2 & T1 are the same for flush component.
        // For merged component T2 is the max timestamp of the latest component, T1 - min timestamp of the earliest.
        // String timestampPattern = "(\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{3})";
        String timestampPattern = "(\\d)";
        StringBuilder regexpStringBuilder = new StringBuilder();
        //non-greedy pattern for storage directory name
        regexpStringBuilder.append(dirPattern).append("+?");
        //partition name
        regexpStringBuilder.append(dirPattern);
        //dataverse name
        regexpStringBuilder.append(dirPattern);
        //dataset & index names
        regexpStringBuilder.append(indexDatasetPattern);
        //component name
        regexpStringBuilder.append(timestampPattern).append(AbstractLSMIndexFileManager.DELIMITER)
                .append(timestampPattern).append(AbstractLSMIndexFileManager.DELIMITER)
                .append(AbstractLSMIndexFileManager.BTREE_SUFFIX);

        Pattern p = Pattern.compile(regexpStringBuilder.toString());
        Matcher m = p.matcher(componentPath);
        if (!m.matches()) {
            throw new HyracksDataException("Cannot parse out component's path");
        }

        List<String> results = new ArrayList<>();
        for (int i = 1; i <= m.groupCount(); i++) {
            results.add(m.group(i));
        }
        return results;
    }

    private void gatherComponentSynopsisStatistics(StatisticsEntry componentSynopses, ILSMDiskComponent newComponent,
            boolean isAntimatter) throws HyracksDataException {
        // Never persist any null synopsis in the disk component metadata page
        if (componentSynopses != null) {
            IndexListStatistics listStatistics = new IndexListStatistics();

            try {
                String componentRelativePath = ((BTree) newComponent.getIndex()).getFileReference().getRelativePath();
                List<String> parsedComponentPath = parsePathComponents(componentRelativePath);
                String dataverse = parsedComponentPath.get(2);
                String dataset = parsedComponentPath.get(3);
                String index = parsedComponentPath.get(4);

                listStatistics.setStatisticsEntry(componentSynopses);
                listStatistics.setStatisticsInfo(dataverse, dataset, index);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }

            if (listStatistics.getStatisticsEntry().getSynopsis() != null) {
                if (!isAntimatter) {
                    newComponent.getMetadata().put(MATTER_STATISTICS_FLAG, listStatistics);
                } else {
                    newComponent.getMetadata().put(ANTIMATTER_STATISTICS_FLAG, listStatistics);
                }
            }
        }
    }

    @Override
    public void persistComponentStatistics(ILSMDiskComponent newComponent) throws HyracksDataException {
        synchronized (synopsisMap) {
            synchronized (antimatterSynopsisMap) {
                gatherComponentSynopsisStatistics(synopsisMap.get(newComponent), newComponent, false);
                gatherComponentSynopsisStatistics(antimatterSynopsisMap.get(newComponent), newComponent, true);
            }
        }
    }

    @Override
    public void sendDiskComponentsStatistics(List<ILSMDiskComponent> diskComponents) throws HyracksDataException {
        INCMessageBroker messageBroker = (INCMessageBroker) ncContext.getMessageBroker();

        List<NonEquiHeightSynopsisElement> componentsSynopsisElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement> componentsAntimatterSynopsisElements = new ArrayList<>();

        String dataverse = "", dataset = "", index = "", field = "";
        long lDomainStart = Long.MAX_VALUE, lDomainEnd = Long.MIN_VALUE;
        double dDomainStart = Double.MAX_VALUE, dDomainEnd = Double.MIN_VALUE;
        StatisticsEntry[] entryLists = new StatisticsEntry[2];

        long tuplesCount = 0, antimatterTuplesCount = 0, totalTuplesSize = 0;
        for (ILSMDiskComponent diskComponent : diskComponents) {
            tuplesCount += ((AbstractLSMDiskComponent) diskComponent).getStatistics().getNumTuples();
            antimatterTuplesCount +=
                    ((AbstractLSMDiskComponent) diskComponent).getStatistics().getNumAntimatterTuples();
            totalTuplesSize += ((AbstractLSMDiskComponent) diskComponent).getStatistics().getTotalTuplesSize();
        }

        SynopsisElementType elementType = SynopsisElementType.Long; // default value
        for (ILSMDiskComponent diskComponent : diskComponents) {
            for (int i = 0; i < 2; i++) {
                IValueReference key = (i != 0) ? ANTIMATTER_STATISTICS_FLAG : MATTER_STATISTICS_FLAG;
                ArrayBackedValueStorage value = new ArrayBackedValueStorage();
                diskComponent.getMetadata().get(key, value);
                byte[] bytes = value.getByteArray();
                IndexListStatistics listStatistics = new IndexListStatistics();
                listStatistics.set(bytes, value.getStartOffset(), value.getLength());

                if (dataverse.equals("") || dataset.equals("") || index.equals("")) {
                    dataverse = listStatistics.getDataverse();
                    dataset = listStatistics.getDataset();
                    index = listStatistics.getIndex();
                }

                StatisticsEntry compStatEntry = listStatistics.getStatisticsEntry();
                entryLists[i] = compStatEntry;
                if (compStatEntry != null) {
                    elementType = entryLists[i].getSynopsis().getElementType();
                }

                lDomainStart = Long.MAX_VALUE;
                lDomainEnd = Long.MIN_VALUE;
                dDomainStart = Double.MAX_VALUE;
                dDomainEnd = Double.MIN_VALUE;
                if (compStatEntry != null) {
                    field = compStatEntry.getField();
                    if (compStatEntry.getSynopsis().getElements().size() > 0) {
                        Number left = compStatEntry.getSynopsis().getElements().get(0).getLeftKey();
                        Number right = compStatEntry.getSynopsis().getElements()
                                .get(compStatEntry.getSynopsis().getElements().size() - 1).getRightKey();
                        switch (elementType) {
                            case Long:
                                lDomainStart = Math.min(lDomainStart, left.longValue());
                                lDomainEnd = Math.max(lDomainEnd, right.longValue());
                                break;
                            case Double:
                                dDomainStart = Math.min(dDomainStart, left.doubleValue());
                                dDomainEnd = Math.max(dDomainEnd, right.doubleValue());
                                break;
                        }
                    }
                    for (Object obj : compStatEntry.getSynopsis().getElements()) {
                        ISynopsisElement<Long> element = (ISynopsisElement) obj;
                        if (i != 0) {
                            componentsAntimatterSynopsisElements.add(new NonEquiHeightSynopsisElement(
                                    element.getLeftKey(), element.getRightKey(), element.getValue()));
                        } else {
                            componentsSynopsisElements.add(new NonEquiHeightSynopsisElement(element.getLeftKey(),
                                    element.getRightKey(), element.getValue()));
                        }
                    }
                }
            }
        }
        ICcAddressedMessage message;
        if (diskComponents.size() > 0) {
            switch (elementType) {
                case Long:
                    entryLists = SynopsisUtils.getCombinedNonEquiHeightSynopses(componentsSynopsisElements,
                            componentsAntimatterSynopsisElements, dataverse, dataset, index, field, lDomainStart,
                            lDomainEnd);
                    break;
                case Double:
                    entryLists = SynopsisUtils.getCombinedNonEquiHeightSynopses(componentsSynopsisElements,
                            componentsAntimatterSynopsisElements, dataverse, dataset, index, field, dDomainStart,
                            dDomainEnd);
                    break;
            }
            if (entryLists[0] != null && entryLists[0].getLength() > 0) {
                entryLists[0].setComponentStats(tuplesCount, antimatterTuplesCount, totalTuplesSize);
            }
            if (entryLists[1] != null && entryLists[1].getLength() > 0) {
                entryLists[1].setComponentStats(tuplesCount, antimatterTuplesCount, totalTuplesSize);
            }
        }
        message = new UpdateStatisticsResponseMessage(entryLists, field);

        try {
            messageBroker.sendMessageToPrimaryCC(message);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void addStatistics(ISynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, ILSMDiskComponent component) throws HyracksDataException {
        synchronized (synopsisMap) {
            synchronized (antimatterSynopsisMap) {
                StatisticsEntry newEntry = new StatisticsEntry(synopsis, field);
                if (isAntimatter) {
                    antimatterSynopsisMap.put(component, newEntry);
                } else {
                    synopsisMap.put(component, newEntry);
                }
                if (component.getState() == ILSMComponent.ComponentState.READABLE_UNWRITABLE) {
                    persistComponentsStatistics(component, newEntry, isAntimatter);
                }
            }
        }
    }

    private void persistComponentsStatistics(ILSMDiskComponent component, StatisticsEntry stats, boolean isAntimatter)
            throws HyracksDataException {
        if (stats != null) {
            gatherComponentSynopsisStatistics(stats, component, isAntimatter);
        }
    }
}