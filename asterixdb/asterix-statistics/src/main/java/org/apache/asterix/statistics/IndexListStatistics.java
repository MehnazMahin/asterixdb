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
package org.apache.asterix.statistics;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class IndexListStatistics extends AbstractPointable implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<StatisticsEntry> statisticsEntries = new ArrayList<>();
    private ArrayBackedValueStorage binaryStats;

    public IndexListStatistics() {
        binaryStats = new ArrayBackedValueStorage();
    }

    public List<StatisticsEntry> getStatisticsEntries() {
        return statisticsEntries;
    }

    public void setStatisticsEntries(List<StatisticsEntry> statsEntries) throws IOException {
        statisticsEntries.addAll(statsEntries);
    }

    @Override
    public byte[] getByteArray() {
        if (binaryStats.getLength() == 0) {
            int binarySize = Integer.BYTES;
            for (StatisticsEntry entry : statisticsEntries) {
                binarySize += Integer.BYTES + entry.getLength();
            }
            binaryStats.setSize(binarySize);
            byte[] bytes = binaryStats.getByteArray();

            int offset = 0;
            IntegerPointable.setInteger(bytes, offset, statisticsEntries.size());
            offset += Integer.BYTES;
            for (StatisticsEntry entry : statisticsEntries) {
                IntegerPointable.setInteger(bytes, offset, entry.getLength());
                offset += Integer.BYTES;
                System.arraycopy(entry.getByteArray(), 0, bytes, offset, entry.getLength());
                offset += entry.getLength();
            }
        }
        length = binaryStats.getLength();
        return binaryStats.getByteArray();
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        if (binaryStats.getLength() <= 0) {
            getByteArray();
        }
        return binaryStats.getLength();
    }

    @Override
    public void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        int offset = start;
        int numOfStats = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;

        for (int i = 0; i < numOfStats; i++) {
            int byteLength = IntegerPointable.getInteger(bytes, offset);
            offset += Integer.BYTES;
            byte[] dummy = new byte[byteLength];
            if (offset - start + byteLength > length) {
                this.length = offset - start - Integer.BYTES;
                binaryStats.reset();
                return;
            }
            System.arraycopy(bytes, offset, dummy, 0, byteLength);
            StatisticsEntry entry = new StatisticsEntry();
            entry.set(dummy, 0, byteLength);
            statisticsEntries.add(entry);
            offset += byteLength;
        }
        this.length = offset - start;
        binaryStats.reset();
    }
}