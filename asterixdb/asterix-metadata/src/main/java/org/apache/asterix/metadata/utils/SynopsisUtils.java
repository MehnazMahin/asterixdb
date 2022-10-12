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
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.ContinuousHistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramSynopsis;
import org.apache.hyracks.storage.am.statistics.historgram.NonEquiHeightHistogramSynopsis;

public class SynopsisUtils {
    public static class NonEquiHeightSynopsisElement implements Comparable<NonEquiHeightSynopsisElement> {
        private long leftBorder;
        private long rightBorder;
        private double value;

        public NonEquiHeightSynopsisElement(long leftBorder, long rightBorder, double value) {
            this.leftBorder = leftBorder;
            this.rightBorder = rightBorder;
            this.value = value;
        }

        public long getLeftBorder() {
            return leftBorder;
        }

        public void setLeftBorder(long leftBorder) {
            this.leftBorder = leftBorder;
        }

        public long getRightBorder() {
            return rightBorder;
        }

        public void setRightBorder(long rightBorder) {
            this.rightBorder = rightBorder;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public int compareTo(NonEquiHeightSynopsisElement element) {
            if (leftBorder != element.leftBorder) {
                return (int) (this.leftBorder - element.leftBorder);
            }
            return (int) (this.rightBorder - element.rightBorder);
        }
    }

    private SynopsisUtils() {
    }

    /*
     * Combines lists of synopsis and antimatter synopsis (even positions contain antimatter synopses),
     * and returns an array of combined equiheight statistics.
     * [0]-->contains combined statistics, [1]-->contains combined antimatter statistics.
     * */
    public static List<StatisticsEntry>[] getCombinedEquiHeightSynopses(
            List<List<StatisticsEntry>> partitionStatEntries, String dataverse, String dataset, String indexName,
            String field) {
        List<NonEquiHeightSynopsisElement> synopsesElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement> antimatterSynopsesElements = new ArrayList<>();

        long domainStart = Integer.MAX_VALUE, domainEnd = Integer.MIN_VALUE;
        for (int i = 0; i < partitionStatEntries.size(); i++) {
            if (partitionStatEntries.get(i).size() == 0) {
                continue;
            }
            if (((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainStart() < domainStart) {
                domainStart = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainStart();
            }
            if (((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainEnd() > domainEnd) {
                domainEnd = ((HistogramSynopsis) partitionStatEntries.get(i).get(0).getSynopsis()).getDomainEnd();
            }
            long leftBorder = domainStart;
            for (ISynopsisElement<Long> element : partitionStatEntries.get(i).get(0).getSynopsis().getElements()) {
                if (i % 2 == 0) {
                    synopsesElements
                            .add(new NonEquiHeightSynopsisElement(leftBorder, element.getKey(), element.getValue()));
                } else {
                    antimatterSynopsesElements
                            .add(new NonEquiHeightSynopsisElement(leftBorder, element.getKey(), element.getValue()));
                }
                leftBorder = element.getKey() + 1;
            }
        }

        List<StatisticsEntry>[] combinedStatistics = new List[2];
        combinedStatistics[0] = new ArrayList<>();
        combinedStatistics[1] = new ArrayList<>();
        int requiredBucketNum = 4;
        if (synopsesElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsesElements, domainStart, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, domainStart);
            //            combinedStatistics[0].add(new StatisticsEntry(new NonEquiHeightHistogramSynopsis<>(domainStart,
            //                    domainEnd, buckets.size(), buckets), dataverse, dataset, indexName, field));
            combinedStatistics[0].add(new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd, 3,
                    equiHeightBuckets.size(), equiHeightBuckets), dataverse, dataset, indexName, field));
        }
        if (antimatterSynopsesElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsesElements, domainStart, domainEnd);
            List<HistogramBucket> equiHeightBuckets =
                    createPseudoEquiHeightSynopses(buckets, requiredBucketNum, domainStart);
            //            combinedStatistics[1].add(new StatisticsEntry(new NonEquiHeightHistogramSynopsis<>(domainStart,
            //                    domainEnd, buckets.size(), buckets), dataverse, dataset, indexName, field));
            combinedStatistics[1].add(new StatisticsEntry(new ContinuousHistogramSynopsis(domainStart, domainEnd, 3,
                    equiHeightBuckets.size(), equiHeightBuckets), dataverse, dataset, indexName, field));
        }
        return combinedStatistics;
    }

    /*
     * Combines both lists of synopsis and antimatter synopsis,
     * and returns an array of combined non-equiheight statistics.
     * [0]-->contains combined statistics, [1]-->contains combined antimatter statistics.
     * */
    public static List<StatisticsEntry>[] getCombinedNonEquiHeightSynopses(
            List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, long domainStart, long domainEnd) {
        List<StatisticsEntry>[] combinedStatistics = new List[2];
        combinedStatistics[0] = new ArrayList<>();
        combinedStatistics[1] = new ArrayList<>();

        if (synopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(synopsisElements, domainStart, domainEnd);
            combinedStatistics[0].add(new StatisticsEntry(
                    new NonEquiHeightHistogramSynopsis<>(domainStart, domainEnd, buckets.size(), buckets), dataverse,
                    dataset, index, field));
        }
        if (antimatterSynopsisElements.size() > 0) {
            List<HistogramBucket> buckets = combineSynopsis(antimatterSynopsisElements, domainStart, domainEnd);
            combinedStatistics[1].add(new StatisticsEntry(
                    new NonEquiHeightHistogramSynopsis<>(domainStart, domainEnd, buckets.size(), buckets), dataverse,
                    dataset, index, field));
        }

        return combinedStatistics;
    }

    private static List<HistogramBucket> combineSynopsis(List<NonEquiHeightSynopsisElement> synopsisElements,
            long domainStart, long domainEnd) {
        List<HistogramBucket> combinedSynopsisElements = new ArrayList<>();
        List<NonEquiHeightSynopsisElement> currentSynopsisElements = new ArrayList<>();
        Collections.sort(synopsisElements);
        NonEquiHeightSynopsisElement currentElement = synopsisElements.get(0);
        long leftBorder = domainStart;
        long rightBorder = currentElement.getRightBorder();
        double value = currentElement.getValue();
        long range = rightBorder - leftBorder + 1;

        boolean lastToBeAdded = false;
        for (int idx = 1; idx < synopsisElements.size(); idx++) {
            while (rightBorder >= synopsisElements.get(idx).getLeftBorder()) {
                currentSynopsisElements.add(synopsisElements.get(idx));
                idx++;
                if (idx == synopsisElements.size()) {
                    idx--;
                    break;
                }
            }

            if (currentSynopsisElements.size() > 0) {
                idx--;
                Collections.sort(currentSynopsisElements);
                if (leftBorder == currentSynopsisElements.get(0).getLeftBorder()
                        && rightBorder > currentSynopsisElements.get(0).getRightBorder()) {
                    currentElement = currentSynopsisElements.get(0);
                    currentSynopsisElements.remove(0);
                    currentSynopsisElements.add(new NonEquiHeightSynopsisElement(leftBorder, rightBorder, value));
                    Collections.sort(currentSynopsisElements);
                    rightBorder = currentElement.getRightBorder();
                    value = currentElement.getValue();
                    range = rightBorder - leftBorder + 1;
                }
            }

            for (NonEquiHeightSynopsisElement element : currentSynopsisElements) {
                if (element.getLeftBorder() > rightBorder) {
                    continue;
                }
                long currentRange = element.getRightBorder() - element.getLeftBorder() + 1;
                double elementsPerBucket = element.getValue() / (double) currentRange;
                value += (elementsPerBucket * range);

                element.setLeftBorder(rightBorder + 1);
                element.setValue(elementsPerBucket * (element.getRightBorder() - element.getLeftBorder() + 1));
            }
            Collections.sort(currentSynopsisElements);
            combinedSynopsisElements.add(new HistogramBucket(rightBorder, value));
            if (rightBorder == domainEnd) {
                break;
            }

            if (currentSynopsisElements.size() > 0) {
                currentElement = currentSynopsisElements.get(0);
                while (currentElement.getLeftBorder() > currentElement.getRightBorder()
                        && currentSynopsisElements.size() > 1) {
                    currentSynopsisElements.remove(0);
                    currentElement = currentSynopsisElements.get(0);
                }
                currentSynopsisElements.remove(0);
                leftBorder = currentElement.getLeftBorder();
                rightBorder = currentElement.getRightBorder();
                range = rightBorder - leftBorder + 1;
                value = currentElement.getValue();
            } else {
                rightBorder = synopsisElements.get(idx).getRightBorder();
                value = synopsisElements.get(idx).getValue();
                if (idx + 1 == synopsisElements.size()) {
                    lastToBeAdded = true;
                }
            }
        }

        if (synopsisElements.size() == 1 || lastToBeAdded) {
            combinedSynopsisElements.add(new HistogramBucket(rightBorder, value));
        }

        return combinedSynopsisElements;
    }

    private static List<HistogramBucket> createPseudoEquiHeightSynopses(List<HistogramBucket> buckets,
            int requiredBucketNum, long domainStart) {
        List<HistogramBucket> equiHeightBuckets = new ArrayList<>();
        if (buckets.size() == 1) {
            equiHeightBuckets.add(buckets.get(0));
        }
        if (buckets.size() > 0) {
            double totalValues = 0.0;
            for (HistogramBucket bucket : buckets) {
                totalValues += bucket.getValue();
            }
            double requiredHeight = totalValues / (double) requiredBucketNum;
            double desiredHeight = requiredHeight, remain = 0.0;
            double value = buckets.get(0).getValue();

            for (int i = 1; i < buckets.size(); i++) {
                boolean insert = false;
                if ((value + buckets.get(i).getValue()) < desiredHeight) {
                    value += buckets.get(i).getValue();
                } else if (value > desiredHeight) {
                    i--;
                    remain = desiredHeight;
                    value -= buckets.get(i).getValue();
                    insert = true;
                } else {
                    remain = desiredHeight - value;
                    insert = true;
                }

                if (insert) {
                    long rightBorder;
                    long leftBorder = (i == 0) ? domainStart : (buckets.get(i - 1).getKey() + 1);
                    double range = buckets.get(i).getKey() - leftBorder + 1;
                    double requiredUnit = remain / buckets.get(i).getValue() * range;
                    if (buckets.get(i).getValue() < remain) {
                        rightBorder = buckets.get(i).getKey();
                    } else {
                        rightBorder = leftBorder + (int) Math.floor(requiredUnit) - 1;
                    }
                    double temp = (double) (rightBorder - leftBorder + 1) / range * buckets.get(i).getValue();
                    desiredHeight = requiredHeight + (remain - temp);
                    equiHeightBuckets.add(new HistogramBucket(rightBorder, value + temp));

                    //reset value for new bucket
                    value = buckets.get(i).getValue() - temp;
                } else if (!insert && (i + 1) == buckets.size()) {
                    equiHeightBuckets.add(new HistogramBucket(buckets.get(i).getKey(), value));
                }
            }
        }
        return equiHeightBuckets;
    }
}