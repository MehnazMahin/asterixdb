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
package org.apache.asterix.metadata.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.asterix.metadata.utils.ISynopsisCombinationsHelper.NonEquiHeightSynopsisElement;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisElementType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramSynopsis;

public class SynopsisUtils {
    private SynopsisUtils() {
    }

    /*
     * Combines lists of synopsis and antimatter synopsis (even positions contain antimatter synopses),
     * and returns an array of combined equiheight statistics.
     * [0]-->contains combined statistics, [1]-->contains combined antimatter statistics.
     * */
    public static List<StatisticsEntry>[] getCombinedEquiHeightSynopses(
            List<List<StatisticsEntry>> partitionStatEntries, String dataverse, String dataset, String indexName,
            String field, int requiredBucketsNum) {
        List<NonEquiHeightSynopsisElement> synopsesElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement> antimatterSynopsesElements = new ArrayList<>();

        List<StatisticsEntry>[] combinedStatistics = new List[2];
        SynopsisElementType elementType = SynopsisElementType.Long;
        for (int i = 0; i < partitionStatEntries.size(); i++) {
            if (partitionStatEntries.get(i).isEmpty()) {
                continue;
            }
            elementType = partitionStatEntries.get(i).get(0).getSynopsis().getElementType();
            break;
        }
        switch (elementType) {
            case Long:
                List<Long> longDomain =
                        getLongDomainStartEnd(partitionStatEntries, synopsesElements, antimatterSynopsesElements);
                combinedStatistics = new SynopsisLongCombinationsHelper().getCombinedEquiHeightSynopses(
                        synopsesElements, antimatterSynopsesElements, dataverse, dataset, indexName, field,
                        longDomain.get(0), longDomain.get(1), requiredBucketsNum);
                break;
            case Double:
                List<Double> doubleDomain =
                        getDoubleDomainStartEnd(partitionStatEntries, synopsesElements, antimatterSynopsesElements);
                combinedStatistics = new SynopsisDoubleCombinationsHelper().getCombinedEquiHeightSynopses(
                        synopsesElements, antimatterSynopsesElements, dataverse, dataset, indexName, field,
                        doubleDomain.get(0), doubleDomain.get(1), requiredBucketsNum);
                break;
        }
        return combinedStatistics;
    }

    /*
     * Combines both lists of synopsis and antimatter synopsis,
     * and returns an array of combined non-equiheight statistics.
     * [0]-->contains combined statistics, [1]-->contains combined antimatter statistics.
     * */
    public static <T> List<StatisticsEntry>[] getCombinedNonEquiHeightSynopses(
            List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, T domainStart, T domainEnd) {

        List<StatisticsEntry>[] combinedStatistics = new List[2];
        if (domainStart instanceof Long) {
            combinedStatistics = new SynopsisLongCombinationsHelper().getCombinedNonEquiHeightSynopses(synopsisElements,
                    antimatterSynopsisElements, dataverse, dataset, index, field, (Long) domainStart, (Long) domainEnd);
        } else if (domainStart instanceof Double) {
            combinedStatistics = new SynopsisDoubleCombinationsHelper().getCombinedNonEquiHeightSynopses(
                    synopsisElements, antimatterSynopsisElements, dataverse, dataset, index, field,
                    (Double) domainStart, (Double) domainEnd);
        }
        return combinedStatistics;
    }

    private static List<Long> getLongDomainStartEnd(List<List<StatisticsEntry>> partitionStatEntries,
            List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements) {
        Long domainStart = Long.MAX_VALUE, domainEnd = Long.MIN_VALUE;
        for (int i = 0; i < partitionStatEntries.size(); i++) {
            if (partitionStatEntries.get(i).size() == 0) {
                continue;
            }
            if ((Long) ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis())
                    .getDomainStart() < domainStart) {
                domainStart = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainStart()
                        .longValue();
            }
            if ((Long) ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis())
                    .getDomainEnd() > domainEnd) {
                domainEnd = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainEnd()
                        .longValue();
            }

            for (ISynopsisElement element : partitionStatEntries.get(i).get(0).getSynopsis().getElements()) {
                if (i % 2 == 0) {
                    synopsisElements.add(new NonEquiHeightSynopsisElement<Long>((Long) element.getLeftKey(),
                            ((Long) element.getRightKey()), element.getValue()));
                } else {
                    antimatterSynopsisElements.add(new NonEquiHeightSynopsisElement<Long>((Long) element.getLeftKey(),
                            ((Long) element.getRightKey()), element.getValue()));
                }
            }
        }
        List<Long> domain = new ArrayList<>();
        domain.add(domainStart);
        domain.add(domainEnd);
        return domain;
    }

    private static List<Double> getDoubleDomainStartEnd(List<List<StatisticsEntry>> partitionStatEntries,
            List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements) {
        Double domainStart = Double.MAX_VALUE, domainEnd = Double.MIN_VALUE;
        for (int i = 0; i < partitionStatEntries.size(); i++) {
            if (partitionStatEntries.get(i).size() == 0) {
                continue;
            }
            if ((Double) ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis())
                    .getDomainStart() < domainStart) {
                domainStart = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainStart()
                        .doubleValue();
            }
            if ((Double) ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis())
                    .getDomainEnd() > domainEnd) {
                domainEnd = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainEnd()
                        .doubleValue();
            }

            for (ISynopsisElement element : partitionStatEntries.get(i).get(0).getSynopsis().getElements()) {
                if (i % 2 == 0) {
                    synopsisElements.add(new NonEquiHeightSynopsisElement<Double>((Double) element.getLeftKey(),
                            ((Double) element.getRightKey()), element.getValue()));
                } else {
                    antimatterSynopsisElements.add(new NonEquiHeightSynopsisElement<Double>(
                            (Double) element.getLeftKey(), ((Double) element.getRightKey()), element.getValue()));
                }
            }
        }
        List<Double> domain = new ArrayList<>();
        domain.add(domainStart);
        domain.add(domainEnd);
        return domain;
    }
}