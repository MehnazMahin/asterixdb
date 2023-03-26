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
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.asterix.metadata.utils.ISynopsisCombinationsHelper.NonEquiHeightSynopsisElement;
import org.apache.asterix.metadata.utils.SynopsisUtils;
import org.apache.asterix.statistics.IndexListStatistics;
import org.apache.asterix.statistics.message.ReportFlushComponentStatisticsMessage;
import org.apache.asterix.statistics.message.ReportMergeComponentStatisticsMessage;
import org.apache.asterix.statistics.message.UpdateStatisticsResponseMessage;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
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
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class StatisticsManager implements IStatisticsManager {

    private final INCServiceContext ncContext;
    //TODO:refactor this to use component IDs instead
    private final MultiValuedMap<ILSMDiskComponent, StatisticsEntry> synopsisMap;
    private final MultiValuedMap<ILSMDiskComponent, StatisticsEntry> antimatterSynopsisMap;

    private final IValueReference MATTER_STATISTICS_FLAG = BooleanPointable.FACTORY.createPointable(false);
    private final IValueReference ANTIMATTER_STATISTICS_FLAG = BooleanPointable.FACTORY.createPointable(true);

    public StatisticsManager(INCServiceContext ncApplicationContext) {
        ncContext = ncApplicationContext;
        synopsisMap = new HashSetValuedHashMap<>();
        antimatterSynopsisMap = new HashSetValuedHashMap<>();
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

    private void gatherComponentSynopsisStatistics(Collection<StatisticsEntry> componentSynopses,
            ILSMDiskComponent newComponent, boolean isAntimatter) throws HyracksDataException {
        if (componentSynopses.size() > 0) {
            IndexListStatistics listStatistics = new IndexListStatistics();
            List<StatisticsEntry> stats = new ArrayList<>(componentSynopses);

            // Never persist any null synopsis in the disk component metadata page
            stats.removeIf(entry -> entry.getSynopsis() == null);
            if (stats.isEmpty()) {
                return;
            }
            try {
                listStatistics.setStatisticsEntries(stats);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }

            if (listStatistics.getStatisticsEntries().get(0).getSynopsis() != null) {
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

        String partition = "", dataverse = "", dataset = "", index = "", field = "";
        Long minCompId = Long.MAX_VALUE, maxCompId = -1L;
        long lDomainStart = Long.MAX_VALUE, lDomainEnd = Long.MIN_VALUE;
        double dDomainStart = Double.MAX_VALUE, dDomainEnd = Double.MIN_VALUE;
        List<StatisticsEntry>[] entryLists = new List[2];
        entryLists[0] = new ArrayList<>();
        entryLists[1] = new ArrayList<>();

        SynopsisElementType elementType = SynopsisElementType.Long; // default value
        for (ILSMDiskComponent diskComponent : diskComponents) {
            // TODO: Find a better way to get the partition instead of parsing
            String componentRelativePath =
                    ((BTree) diskComponent.getIndex()).getFileReference().getRelativePath();
            List<String> parsedComponentPath = parsePathComponents(componentRelativePath);
            partition = parsedComponentPath.get(1);
            dataverse = parsedComponentPath.get(2);
            dataset = parsedComponentPath.get(3);
            index = parsedComponentPath.get(4);
            if (minCompId > Long.parseLong(parsedComponentPath.get(5))) {
                minCompId = Long.parseLong(parsedComponentPath.get(5));
            }
            if (maxCompId < Long.parseLong(parsedComponentPath.get(6))) {
                maxCompId = Long.parseLong(parsedComponentPath.get(6));
            }

            for (int i = 0; i < 2; i++) {
                IValueReference key = (i != 0) ? ANTIMATTER_STATISTICS_FLAG : MATTER_STATISTICS_FLAG;
                ArrayBackedValueStorage value = new ArrayBackedValueStorage();
                diskComponent.getMetadata().get(key, value);
                byte[] bytes = value.getByteArray();
                IndexListStatistics listStatistics = new IndexListStatistics();
                listStatistics.set(bytes, value.getStartOffset(), value.getLength());

                List<StatisticsEntry> compStatEntries = listStatistics.getStatisticsEntries();
                if (!listStatistics.getStatisticsEntries().isEmpty()) {
                    entryLists[i] = listStatistics.getStatisticsEntries();
                    elementType = entryLists[i].get(0).getSynopsis().getElementType();
                }

                lDomainStart = Long.MAX_VALUE;
                lDomainEnd = Long.MIN_VALUE;
                dDomainStart = Double.MAX_VALUE;
                dDomainEnd = Double.MIN_VALUE;
                for (StatisticsEntry entry : compStatEntries) {
                    field = entry.getField();
                    if (entry.getSynopsis().getElements().size() > 0) {
                        Number left = entry.getSynopsis().getElements().get(0).getLeftKey();
                        Number right = entry.getSynopsis().getElements().get(
                                entry.getSynopsis().getElements().size() - 1).getRightKey();
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
                    for (Object obj : entry.getSynopsis().getElements()) {
                        ISynopsisElement<Long> element = (ISynopsisElement) obj;
                        if (i != 0) {
                            componentsAntimatterSynopsisElements.add(new NonEquiHeightSynopsisElement(
                                    element.getLeftKey(), element.getRightKey(), element.getValue()));
                        } else {
                            componentsSynopsisElements.add(new NonEquiHeightSynopsisElement(
                                    element.getLeftKey(), element.getRightKey(), element.getValue()));
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
                            componentsAntimatterSynopsisElements, dataverse, dataset, index, field,
                            lDomainStart, lDomainEnd);
                    break;
                case Double:
                    entryLists = SynopsisUtils.getCombinedNonEquiHeightSynopses(componentsSynopsisElements,
                            componentsAntimatterSynopsisElements, dataverse, dataset, index, field,
                            dDomainStart, dDomainEnd);
                    break;
            }
            message = new UpdateStatisticsResponseMessage(entryLists, ncContext.getNodeId(), partition,
                    new ComponentStatisticsId(minCompId, maxCompId), field);
        } else {
            message = new UpdateStatisticsResponseMessage(entryLists, ncContext.getNodeId(), partition,
                    new ComponentStatisticsId(0L, 0L), field);
        }
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
                StatisticsEntry newEntry = new StatisticsEntry(synopsis, dataverse, dataset, index, field);
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
        List<StatisticsEntry> entries = new ArrayList<>();
        if (stats != null) {
            entries.add(stats);
            gatherComponentSynopsisStatistics(entries, component, isAntimatter);
        }
    }


    // TODO : Delete this function
    private void sendMessage(ICcAddressedMessage msg) throws HyracksDataException {
        //TODO: make message sending routine asynchronous?
        try {
            ((INCMessageBroker) ncContext.getMessageBroker()).sendMessageToPrimaryCC(msg);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    // TODO : Delete this function
    private void sendFlushSynopsisStatistics(Collection<StatisticsEntry> flushComponentSynopses,
            ILSMDiskComponent newComponent, boolean isAntimatter) throws HyracksDataException {
        for (StatisticsEntry flushComponentSynopsis : flushComponentSynopses) {
            // send message only about non-empty statistics
            if (flushComponentSynopsis != null) {
                List<String> parsedComponentsPath =
                        parsePathComponents(((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
                ICcAddressedMessage msg = new ReportFlushComponentStatisticsMessage(flushComponentSynopsis,
                        ncContext.getNodeId(), parsedComponentsPath.get(1),
                        new ComponentStatisticsId(Long.parseLong(parsedComponentsPath.get(6)),
                                Long.parseLong(parsedComponentsPath.get(5))),
                        isAntimatter);
                sendMessage(msg);
            }
        }
    }

    // TODO : Delete this function
    private void sendMergeSynopsisStatistics(Collection<StatisticsEntry> flushComponentSynopses,
            ILSMDiskComponent newComponent, List<ILSMDiskComponent> mergedComponents, boolean isAntimatter)
            throws HyracksDataException {
        for (StatisticsEntry flushComponentSynopsis : flushComponentSynopses) {
            List<String> parsedComponentsPath =
                    parsePathComponents(((BTree) newComponent.getIndex()).getFileReference().getRelativePath());
            List<ComponentStatisticsId> mergedComponentIds = new ArrayList<>(mergedComponents.size());
            for (ILSMDiskComponent mergedComponent : mergedComponents) {
                List<String> parsedMergedComponentPath =
                        parsePathComponents(((BTree) mergedComponent.getIndex()).getFileReference().getRelativePath());
                mergedComponentIds.add(new ComponentStatisticsId(Long.parseLong(parsedMergedComponentPath.get(6)),
                        Long.parseLong(parsedMergedComponentPath.get(5))));
            }
            ICcAddressedMessage msg = new ReportMergeComponentStatisticsMessage(flushComponentSynopsis,
                    ncContext.getNodeId(), parsedComponentsPath.get(1),
                    new ComponentStatisticsId(Long.parseLong(parsedComponentsPath.get(6)),
                            Long.parseLong(parsedComponentsPath.get(5))),
                    isAntimatter, mergedComponentIds);
            sendMessage(msg);
        }
    }

    // TODO : Delete this function
    @Override
    public void sendFlushStatistics(ILSMDiskComponent flushedComponent) throws HyracksDataException {
        synchronized (synopsisMap) {
            synchronized (antimatterSynopsisMap) {
                sendFlushSynopsisStatistics(synopsisMap.remove(flushedComponent), flushedComponent, false);
                sendFlushSynopsisStatistics(antimatterSynopsisMap.remove(flushedComponent), flushedComponent, true);
            }
        }
    }

    // TODO : Delete this function
    @Override
    public void sendMergeStatistics(ILSMDiskComponent newComponent, List<ILSMDiskComponent> mergedComponents)
            throws HyracksDataException {
        synchronized (synopsisMap) {
            synchronized (antimatterSynopsisMap) {
                sendMergeSynopsisStatistics(synopsisMap.remove(newComponent), newComponent, mergedComponents, false);
                sendMergeSynopsisStatistics(antimatterSynopsisMap.remove(newComponent), newComponent, mergedComponents,
                        true);
            }
        }
    }
}