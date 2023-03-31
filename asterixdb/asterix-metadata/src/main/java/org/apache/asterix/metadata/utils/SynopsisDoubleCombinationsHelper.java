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

public class SynopsisDoubleCombinationsHelper implements ISynopsisCombinationsHelper<Double> {
    public SynopsisDoubleCombinationsHelper() {
    }

    @Override
    public Double getBucketSpan(NonEquiHeightSynopsisElement element) {
        return element.getRightKey().doubleValue() - element.getLeftKey().doubleValue();
    }

    @Override
    public StatisticsEntry[] getCombinedEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, Double domainStart, Double domainEnd, int requiredBucketNum) {

        StatisticsEntry[] combinedStatistics = new StatisticsEntry[2];
        Long[] totalTuplesNumber = { -1L };

        if (synopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsisElements, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, totalTuplesNumber);
            combinedStatistics[0] = new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Double, totalTuplesNumber[0], equiHeightBuckets.size(), equiHeightBuckets),
                    field);
        }
        if (antimatterSynopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsisElements, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, totalTuplesNumber);
            combinedStatistics[1] = new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Double, totalTuplesNumber[0], equiHeightBuckets.size(), equiHeightBuckets),
                    field);
        }
        return combinedStatistics;
    }

    @Override
    public StatisticsEntry[] getCombinedNonEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, Double domainStart, Double domainEnd) {

        StatisticsEntry[] combinedStatistics = new StatisticsEntry[2];

        if (synopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsisElements, domainEnd);
            combinedStatistics[0] = new StatisticsEntry(new NonEquiHeightHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Double, buckets.size(), buckets), field);
        }
        if (antimatterSynopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsisElements, domainEnd);
            combinedStatistics[1] = new StatisticsEntry(new NonEquiHeightHistogramSynopsis(domainStart, domainEnd,
                    SynopsisElementType.Double, buckets.size(), buckets), field);
        }

        return combinedStatistics;
    }

    @Override
    public List<HistogramBucket> combineSynopsis(List<NonEquiHeightSynopsisElement> synopsisElements,
            Double domainEnd) {

        List<HistogramBucket> combinedSynopsisElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement<Double>> currentSynopsisElements = new ArrayList<>();
        Collections.sort(synopsisElements);
        NonEquiHeightSynopsisElement<Double> currentElement = synopsisElements.get(0);
        double leftBorder = currentElement.getLeftKey().doubleValue();
        double rightBorder = currentElement.getRightKey().doubleValue();
        double value = currentElement.getValue();
        double range = getBucketSpan(currentElement);

        boolean lastToBeAdded = false;
        for (int idx = 1; idx < synopsisElements.size(); idx++) {
            while (rightBorder >= synopsisElements.get(idx).getLeftKey().doubleValue()) {
                currentSynopsisElements.add(synopsisElements.get(idx));
                idx++;
                if (idx == synopsisElements.size()) {
                    break;
                }
            }

            idx--;
            Collections.sort(currentSynopsisElements);
            for (int j = 0; j < currentSynopsisElements.size(); j++) {
                if (currentSynopsisElements.get(j).getLeftKey().doubleValue() >= leftBorder
                        && rightBorder > currentSynopsisElements.get(j).getRightKey().doubleValue()) {
                    currentSynopsisElements.add(currentElement);
                    currentElement = currentSynopsisElements.get(j);
                    currentSynopsisElements.remove(j);
                    j--;
                    rightBorder = currentElement.getRightKey().doubleValue();
                    value = currentElement.getValue();
                    range = rightBorder - leftBorder;
                }
            }

            for (NonEquiHeightSynopsisElement<Double> element : currentSynopsisElements) {
                if (element.getLeftKey().doubleValue() > rightBorder) {
                    continue;
                }
                double currentRange = getBucketSpan(element);
                currentRange = (currentRange == 0.0 && element.getValue() > 0.0) ? 1.0 : currentRange;
                double partialRange = element.getLeftKey().doubleValue() > leftBorder
                        ? rightBorder - element.getLeftKey().doubleValue() : range;
                element.setLeftBorder(rightBorder);
                if (currentRange > 0.0) {
                    double elementsPerBucket = element.getValue() / currentRange;
                    value += (elementsPerBucket * partialRange);
                    element.setValue(element.getValue() - elementsPerBucket * partialRange);
                }
            }

            for (int j = 0; j < currentSynopsisElements.size(); j++) {
                if (currentSynopsisElements.get(j).getLeftKey().doubleValue() == leftBorder
                        && currentSynopsisElements.get(j).getRightKey().doubleValue() == rightBorder
                        && leftBorder == rightBorder) {
                    value += currentSynopsisElements.get(j).getValue();
                    currentSynopsisElements.remove(j);
                    j--;
                } else if (currentSynopsisElements.get(j).getLeftKey().doubleValue() == currentSynopsisElements.get(j)
                        .getRightKey().doubleValue() && currentSynopsisElements.get(j).getValue() == 0.0) {
                    currentSynopsisElements.remove(j);
                    j--;
                } else
                    break;
            }
            Collections.sort(currentSynopsisElements);
            combinedSynopsisElements.add(new HistogramBucket<>(leftBorder, rightBorder, value));
            if (rightBorder == domainEnd) {
                break;
            }

            if (currentSynopsisElements.size() > 0) {
                currentElement = currentSynopsisElements.get(0);
                while (currentElement.getLeftKey().doubleValue() > currentElement.getRightKey().doubleValue()
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
            leftBorder = currentElement.getLeftKey().doubleValue();
            rightBorder = currentElement.getRightKey().doubleValue();
            range = getBucketSpan(currentElement);
            value = currentElement.getValue();
        }

        if (synopsisElements.size() == 1 || lastToBeAdded) {
            for (NonEquiHeightSynopsisElement<Double> element : currentSynopsisElements) {
                value += element.getValue();
                rightBorder = element.getRightKey().doubleValue();
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
            double consideredValues = 0.0;

            double leftBorder = buckets.get(0).getLeftKey().doubleValue();
            double rightBorder = buckets.get(0).getRightKey().doubleValue();
            for (int i = 1; i < buckets.size(); i++) {
                boolean insert = false;
                if ((value + buckets.get(i).getValue()) < desiredHeight) {
                    rightBorder = buckets.get(i).getRightKey().doubleValue();
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
                    double range =
                            buckets.get(i).getRightKey().doubleValue() - buckets.get(i).getLeftKey().doubleValue();
                    range = (range == 0.0 && buckets.get(i).getValue() > 0.0) ? 1.0 : range;
                    double requiredUnit = remain / buckets.get(i).getValue();
                    requiredUnit = (range > 0.0) ? (requiredUnit * range) : requiredUnit;

                    if (Math.abs(buckets.get(i).getValue() - remain) < 0.0000000001) {
                        rightBorder = buckets.get(i).getRightKey().doubleValue();
                    } else {
                        if (buckets.get(i).getLeftKey().doubleValue() == buckets.get(i).getRightKey().doubleValue()
                                && buckets.get(i).getValue() > 0.0) {
                            requiredUnit = Math.floor(requiredUnit);
                        }
                        rightBorder = leftBorder + requiredUnit;
                        if (leftBorder < buckets.get(i).getLeftKey().doubleValue()) {
                            rightBorder = buckets.get(i).getLeftKey().doubleValue() + requiredUnit;
                        }
                    }

                    double partialRange = (rightBorder == leftBorder) ? 1.0 : (rightBorder - leftBorder);
                    if (leftBorder < buckets.get(i).getLeftKey().doubleValue()) {
                        partialRange = rightBorder - buckets.get(i).getLeftKey().doubleValue();
                    }
                    double temp = partialRange / range * buckets.get(i).getValue();
                    if (remain < desiredHeight) {
                        consideredValues = 0;
                    }
                    consideredValues += temp;

                    if (value > desiredHeight) {
                        equiHeightBuckets.add(new HistogramBucket<>(leftBorder, rightBorder, temp));
                    } else {
                        if (temp == 0.0 && i > 0) {
                            if ((i + 1) == buckets.size() && equiHeightBuckets.size() == requiredBucketNum - 1) {
                                equiHeightBuckets.add(new HistogramBucket<>(leftBorder,
                                        buckets.get(i).getRightKey().doubleValue(), value + buckets.get(i).getValue()));
                                continue;
                            }
                            equiHeightBuckets.add(new HistogramBucket<>(leftBorder,
                                    buckets.get(i - 1).getRightKey().doubleValue(), value));
                        } else {
                            equiHeightBuckets.add(new HistogramBucket<>(leftBorder, rightBorder, value + temp));
                        }
                    }
                    leftBorder = rightBorder;
                    rightBorder = buckets.get(i).getRightKey().doubleValue();
                    value = buckets.get(i).getValue() - consideredValues;
                    if (value == 0 && (i + 1) < buckets.size()) { // move forward to the next bucket
                        i++;
                        leftBorder = buckets.get(i).getLeftKey().doubleValue();
                        rightBorder = buckets.get(i).getRightKey().doubleValue();
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
