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
package org.apache.hyracks.storage.am.statistics.historgram;

import java.util.List;

import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;

public abstract class HistogramSynopsis<T extends HistogramBucket> extends AbstractSynopsis {
    protected final SynopsisElementType synopsisElementType;

    public HistogramSynopsis(Number domainStart, Number domainEnd, SynopsisElementType type, int bucketsNum,
            List<T> synopsisElements) {
        super(domainStart, domainEnd, bucketsNum, synopsisElements);
        this.synopsisElementType = type;
    }

    //implicit cast to operate with buckets as a list
    protected List<T> getBuckets() {
        return (List<T>) synopsisElements;
    }

    @Override
    public SynopsisElementType getElementType() {
        return synopsisElementType;
    }

    public abstract void appendToBucket(int bucketId, Number currTupleValue, double frequency);

    public abstract boolean advanceBucket(int activeBucket, int activeBucketElementsNum, Number currTupleValue,
            Number lastAddedTupleValue);

    public void finishBucket(int activeBucket, Number lastValueAdded) {
    }
}