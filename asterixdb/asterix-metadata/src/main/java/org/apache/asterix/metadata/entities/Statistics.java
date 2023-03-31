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
package org.apache.asterix.metadata.entities;

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramSynopsis;

/**
 * Metadata describing a statistics entity.
 */
public class Statistics implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final DataverseName dataverseName;
    private final String dataset;
    private final String index;
    private final boolean isAntimatter;
    private final String field;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Statistics that = (Statistics) o;
        boolean flag = temp == that.temp && Objects.equals(dataverseName, that.dataverseName)
                && Objects.equals(dataset, that.dataset) && Objects.equals(index, that.index)
                && isAntimatter == that.isAntimatter && Objects.equals(field, that.field);

        if (flag) {
            if (synopsis.getElements() != null && that.synopsis.getElements() != null
                    && synopsis.getType().equals(that.synopsis.getType())) {
                switch (synopsis.getType()) {
                    case ContinuousHistogram:
                        List<HistogramBucket> elements = ((HistogramSynopsis) synopsis).getElements();
                        List<HistogramBucket> thatElements = ((HistogramSynopsis) that.synopsis).getElements();
                        if (elements.size() == thatElements.size()) {
                            for (int i = 0; i < elements.size(); i++) {
                                if (!elements.get(i).getLeftKey().equals(thatElements.get(i).getLeftKey())
                                        || !elements.get(i).getRightKey().equals(thatElements.get(i).getRightKey())
                                        || elements.get(i).getValue() != thatElements.get(i).getValue()) {
                                    return false;
                                }
                            }
                            return true;
                        }
                        return false;
                    case QuantileSketch:
                        flag = Objects.equals(synopsis, that.synopsis);
                }
            }
        }
        return flag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, dataset, index, field, temp, isAntimatter, synopsis);
    }

    private final boolean temp;
    private final ISynopsis<? extends ISynopsisElement<? extends Number>> synopsis;

    public Statistics(DataverseName dataverseName, String dataset, String index, boolean temp, boolean isAntimatter,
            String field, ISynopsis<? extends ISynopsisElement<? extends Number>> synopsis) {
        this.dataverseName = dataverseName;
        this.dataset = dataset;
        this.index = index;
        this.field = field;
        this.temp = temp;
        this.isAntimatter = isAntimatter;
        this.synopsis = synopsis;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return dataset;
    }

    public String getIndexName() {
        return index;
    }

    public String getFieldName() {
        return field;
    }

    public boolean isTemp() {
        return temp;
    }

    public boolean isAntimatter() {
        return isAntimatter;
    }

    public ISynopsis<? extends ISynopsisElement<? extends Number>> getSynopsis() {
        return synopsis;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addStatisticsToCache(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropStatistics(this);
    }
}