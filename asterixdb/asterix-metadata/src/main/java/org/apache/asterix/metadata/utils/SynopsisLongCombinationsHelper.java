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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisElementType;
import org.apache.hyracks.storage.am.statistics.historgram.ContinuousHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.NonEquiHeightHistogramSynopsis;

public class SynopsisLongCombinationsHelper implements ISynopsisCombinationsHelper<Long> {
    public SynopsisLongCombinationsHelper() {
    }

    @Override
    public Long getBucketSpan(NonEquiHeightSynopsisElement element) {
        return element.getRightKey().longValue() - element.getLeftKey().longValue() + 1;
    }

    @Override
    public StatisticsEntry[] getCombinedEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, Long domainStart, Long domainEnd, int requiredBucketNum) {

        StatisticsEntry[] combinedStatistics = new StatisticsEntry[2];
        Long[] totalTuplesNum = { -1L };

        if (synopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsisElements, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, totalTuplesNum);
            combinedStatistics[0] = new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Long, totalTuplesNum[0], equiHeightBuckets.size(), equiHeightBuckets), field);
        }
        if (antimatterSynopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsisElements, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, totalTuplesNum);
            combinedStatistics[1] = new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Long, totalTuplesNum[0], equiHeightBuckets.size(), equiHeightBuckets), field);
        }
        return combinedStatistics;
    }

    @Override
    public StatisticsEntry[] getCombinedNonEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, Long domainStart, Long domainEnd) {

        StatisticsEntry[] combinedStatistics = new StatisticsEntry[2];

        if (synopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsisElements, domainEnd);
            combinedStatistics[0] = new StatisticsEntry(new NonEquiHeightHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Long, buckets.size(), buckets), field);
        }
        if (antimatterSynopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsisElements, domainEnd);
            combinedStatistics[1] = new StatisticsEntry(new NonEquiHeightHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Long, buckets.size(), buckets), field);
        }

        return combinedStatistics;
    }

    @Override
    public List<HistogramBucket> combineSynopsis(List<NonEquiHeightSynopsisElement> synopsisElements, Long domainEnd) {

        List<HistogramBucket> combinedSynopsisElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement<Long>> currentSynopsisElements = new ArrayList<>();
        Collections.sort(synopsisElements);
        NonEquiHeightSynopsisElement<Long> currentElement = synopsisElements.get(0);
        long leftBorder = currentElement.getLeftKey().longValue();
        long rightBorder = currentElement.getRightKey().longValue();
        double value = currentElement.getValue();
        long range = getBucketSpan(currentElement);

        boolean lastToBeAdded = false;
        for (int idx = 1; idx < synopsisElements.size(); idx++) {
            while (rightBorder >= synopsisElements.get(idx).getLeftKey().longValue()) {
                currentSynopsisElements.add(synopsisElements.get(idx));
                idx++;
                if (idx == synopsisElements.size()) {
                    break;
                }
            }

            idx--;
            Collections.sort(currentSynopsisElements);
            for (int j = 0; j < currentSynopsisElements.size(); j++) {
                if (currentSynopsisElements.get(j).getLeftKey().longValue() >= leftBorder
                        && rightBorder > currentSynopsisElements.get(j).getRightKey().longValue()) {
                    currentSynopsisElements.add(currentElement);
                    currentElement = currentSynopsisElements.get(j);
                    currentSynopsisElements.remove(j);
                    j--;
                    rightBorder = currentElement.getRightKey().longValue();
                    value = currentElement.getValue();
                    range = rightBorder - leftBorder + 1;
                }
            }

            for (NonEquiHeightSynopsisElement<Long> element : currentSynopsisElements) {
                if (element.getLeftKey().longValue() > rightBorder) {
                    continue;
                }
                long currentRange = getBucketSpan(element);
                long partialRange = element.getRightKey().longValue() > leftBorder
                        ? (rightBorder - element.getLeftKey().longValue() + 1) : range;
                double elementsPerBucket = element.getValue() / (double) currentRange;
                value += (elementsPerBucket * partialRange);

                element.setLeftBorder(rightBorder + 1);
                element.setValue(element.getValue() - elementsPerBucket * partialRange);
            }
            Collections.sort(currentSynopsisElements);
            combinedSynopsisElements.add(new HistogramBucket<>(leftBorder, rightBorder, value));
            if (rightBorder == domainEnd) {
                break;
            }

            if (currentSynopsisElements.size() > 0) {
                currentElement = currentSynopsisElements.get(0);
                while (currentElement.getLeftKey().longValue() > currentElement.getRightKey().longValue()
                        && currentSynopsisElements.size() > 1) {
                    currentSynopsisElements.remove(0);
                    currentElement = currentSynopsisElements.get(0);
                }
                currentSynopsisElements.remove(0);
                if (idx + 1 == synopsisElements.size()) {
                    lastToBeAdded = true;
                }
            } else {
                if (idx + 1 == synopsisElements.size()) {
                    continue;
                }
                currentElement = synopsisElements.get(++idx);
                if (idx + 1 == synopsisElements.size()) {
                    lastToBeAdded = true;
                }
            }
            leftBorder = currentElement.getLeftKey().longValue();
            rightBorder = currentElement.getRightKey().longValue();
            range = getBucketSpan(currentElement);
            value = currentElement.getValue();
        }

        if (synopsisElements.size() == 1 || lastToBeAdded) {
            for (NonEquiHeightSynopsisElement<Long> element : currentSynopsisElements) {
                value += element.getValue();
                rightBorder = element.getRightKey().longValue();
            }
            combinedSynopsisElements.add(new HistogramBucket<>(leftBorder, rightBorder, value));
        }

        return combinedSynopsisElements;
    }

    @Override
    public List<HistogramBucket> createPseudoEquiHeightSynopses(List<HistogramBucket> buckets, int requiredBucketNum,
            Long[] totalTuplesNum) {

        List<HistogramBucket> equiHeightBuckets = new ArrayList<>();
        if (buckets.size() == 1) {
            equiHeightBuckets.add(buckets.get(0));
            return equiHeightBuckets;
        }
        if (buckets.size() > 0) {
            double totalValues = 0.0;
            for (HistogramBucket bucket : buckets) {
                totalValues += bucket.getValue();
            }
            totalTuplesNum[0] = (long) totalValues;

            double requiredHeight = totalValues / (double) requiredBucketNum;
            double desiredHeight = requiredHeight, remain = 0.0;
            double value = buckets.get(0).getValue();
            double consideredValues = 0;

            long leftBorder = buckets.get(0).getLeftKey().longValue();
            long rightBorder = buckets.get(0).getRightKey().longValue();
            for (int i = 1; i < buckets.size(); i++) {
                boolean insert = false;
                if ((value + buckets.get(i).getValue()) < desiredHeight) {
                    rightBorder = buckets.get(i).getRightKey().longValue();
                    value += buckets.get(i).getValue();
                } else if (value > desiredHeight) {
                    i--;
                    remain = desiredHeight;
                    insert = true;
                } else {
                    remain = desiredHeight - value;
                    insert = true;
                }

                if (insert) {
                    long range = buckets.get(i).getRightKey().longValue() - buckets.get(i).getLeftKey().longValue() + 1;
                    double requiredUnit = remain / buckets.get(i).getValue();
                    requiredUnit = (range > 0) ? (requiredUnit * (double) range) : requiredUnit;

                    if (Math.abs(buckets.get(i).getValue() - remain) < 0.0000001) {
                        rightBorder = buckets.get(i).getRightKey().longValue();
                    } else {
                        requiredUnit =
                                Math.floor(requiredUnit) > 0 ? Math.floor(requiredUnit) - 1 : Math.floor(requiredUnit);
                        rightBorder = leftBorder + (long) requiredUnit;
                        if (leftBorder < buckets.get(i).getLeftKey().longValue()) {
                            rightBorder = buckets.get(i).getLeftKey().longValue() + (long) requiredUnit;
                        }
                    }

                    long partialRange = rightBorder - leftBorder + 1;
                    if (leftBorder < buckets.get(i).getLeftKey().longValue()) {
                        partialRange = rightBorder - buckets.get(i).getLeftKey().longValue() + 1;
                    }
                    double temp = (double) partialRange / range * buckets.get(i).getValue();
                    if (remain < desiredHeight) {
                        consideredValues = 0;
                    }
                    consideredValues += temp;
                    if (value > desiredHeight) {
                        equiHeightBuckets.add(new HistogramBucket<>(leftBorder, rightBorder, temp));
                    } else {
                        equiHeightBuckets.add(new HistogramBucket<>(leftBorder, rightBorder, value + temp));
                    }
                    leftBorder = rightBorder + 1;
                    rightBorder = buckets.get(i).getRightKey().longValue();
                    value = buckets.get(i).getValue() - consideredValues;
                    if (value == 0 && (i + 1) < buckets.size()) { // move forward to the next bucket
                        i++;
                        leftBorder = buckets.get(i).getLeftKey().longValue();
                        rightBorder = buckets.get(i).getRightKey().longValue();
                        value = buckets.get(i).getValue();
                        consideredValues = 0;
                    } else {
                        desiredHeight = requiredHeight + (remain - temp);
                    }
                }
            }
            if (equiHeightBuckets.size() < requiredBucketNum) {
                equiHeightBuckets.add(new HistogramBucket<>(leftBorder, rightBorder, value));
            }
        }
        return equiHeightBuckets;
    }
}
