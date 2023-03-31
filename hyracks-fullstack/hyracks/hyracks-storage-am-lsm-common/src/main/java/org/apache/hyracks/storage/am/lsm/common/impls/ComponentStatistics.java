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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;

public class ComponentStatistics {
    public static final UTF8StringPointable NUM_TUPLES_KEY = UTF8StringPointable.generateUTF8Pointable("NumTuples");
    public static final UTF8StringPointable NUM_ANTIMATTER_TUPLES_KEY =
            UTF8StringPointable.generateUTF8Pointable("NumAntimatterTuples");
    public static final UTF8StringPointable TOTAL_TUPLES_SIZE_KEY =
            UTF8StringPointable.generateUTF8Pointable("TotalTuplesSize");
    private static final LongPointable pointable = LongPointable.FACTORY.createPointable();
    private final ArrayBackedValueStorage buffer = new ArrayBackedValueStorage(Long.BYTES);

    private long numTuples;
    private long numAntimatterTuples;
    private long totalTuplesSize;

    public ComponentStatistics(long numTuples, long numAntimatterTuples, long totalTuplesSize) {
        this.numTuples = numTuples;
        this.numAntimatterTuples = numAntimatterTuples;
        this.totalTuplesSize = totalTuplesSize;
    }

    public long getNumTuples() {
        return numTuples;
    }

    public long getNumAntimatterTuples() {
        return numAntimatterTuples;
    }

    public long getTotalTuplesSize() {
        return totalTuplesSize;
    }

    public void resetTuples(long numTuples, long tuplesSize) {
        this.numTuples = numTuples;
        adjustTotalTuplesSize(tuplesSize);
    }

    public void resetAntimatterTuples(long numAntimatterTuples, long tuplesSize) {
        this.numAntimatterTuples = numAntimatterTuples;
        if (totalTuplesSize > 0 && tuplesSize > 0) {
            adjustTotalTuplesSize(tuplesSize * -1);
        }
    }

    private void adjustTotalTuplesSize(long totalTuplesSize) {
        this.totalTuplesSize += totalTuplesSize;
    }

    public void readTuplesNum(IComponentMetadata metadata) throws HyracksDataException {
        numTuples = ComponentUtils.getLong(metadata, pointable, 0L, buffer);
        numAntimatterTuples = ComponentUtils.getLong(metadata, NUM_ANTIMATTER_TUPLES_KEY, 0L, buffer);
    }

    public void readTotalTuplesSize(IComponentMetadata metadata) throws HyracksDataException {
        totalTuplesSize = ComponentUtils.getLong(metadata, TOTAL_TUPLES_SIZE_KEY, 0L, buffer);
    }

    public void writeTuplesNum(IComponentMetadata metadata) throws HyracksDataException {
        pointable.setLong(numTuples);
        metadata.put(NUM_TUPLES_KEY, pointable);
        pointable.setLong(numAntimatterTuples);
        metadata.put(NUM_ANTIMATTER_TUPLES_KEY, pointable);
    }

    public void writeTotalTuplesSize(IComponentMetadata metadata) throws HyracksDataException {
        pointable.setLong(totalTuplesSize);
        metadata.put(TOTAL_TUPLES_SIZE_KEY, pointable);
    }
}
