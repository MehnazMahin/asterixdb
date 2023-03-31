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

import java.util.List;

import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;

public interface ISynopsisCombinationsHelper<T extends Number> {
    class NonEquiHeightSynopsisElement<T extends Number> extends HistogramBucket
            implements Comparable<NonEquiHeightSynopsisElement> {
        public NonEquiHeightSynopsisElement(T leftBorder, T rightBorder, double value) {
            super(leftBorder, rightBorder, value);
        }

        @Override
        public int compareTo(NonEquiHeightSynopsisElement element) {
            if (this.getLeftKey() instanceof Long) {
                if (this.getLeftKey().longValue() != element.getLeftKey().longValue()) {
                    return (int) (this.getLeftKey().longValue() - element.getLeftKey().longValue());
                }
                return (int) (this.getRightKey().longValue() - element.getRightKey().longValue());
            }
            if (this.getLeftKey().doubleValue() != element.getLeftKey().doubleValue()) {
                return Double.compare(this.getLeftKey().doubleValue(), element.getLeftKey().doubleValue());
            }
            return Double.compare(this.getRightKey().doubleValue(), element.getRightKey().doubleValue());
        }
    }

    T getBucketSpan(NonEquiHeightSynopsisElement element);

    StatisticsEntry[] getCombinedEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, T domainStart, T domainEnd, int requiredBucketNum);

    StatisticsEntry[] getCombinedNonEquiHeightSynopses(List<NonEquiHeightSynopsisElement> synopsisElements,
            List<NonEquiHeightSynopsisElement> antimatterSynopsisElements, String dataverse, String dataset,
            String index, String field, T domainStart, T domainEnd);

    List<HistogramBucket> combineSynopsis(List<NonEquiHeightSynopsisElement> synopsisElements, T domainEnd);

    List<HistogramBucket> createPseudoEquiHeightSynopses(List<HistogramBucket> buckets, int requiredBucketNum,
            Long[] totalTuplesNum);
}
