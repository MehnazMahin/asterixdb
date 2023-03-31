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
import java.nio.charset.StandardCharsets;

import org.apache.asterix.metadata.declared.StatisticsEntry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class IndexListStatistics extends AbstractPointable implements Serializable {
    private static final long serialVersionUID = 1L;

    private String dataverse;
    private String dataset;
    private String index;
    private StatisticsEntry statisticsEntry;
    private ArrayBackedValueStorage binaryStats;

    public IndexListStatistics() {
        binaryStats = new ArrayBackedValueStorage();
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getDataset() {
        return dataset;
    }

    public String getIndex() {
        return index;
    }

    public StatisticsEntry getStatisticsEntry() {
        return statisticsEntry;
    }

    public void setStatisticsInfo(String dataverse, String dataset, String index) {
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
    }

    public void setStatisticsEntry(StatisticsEntry statsEntry) throws IOException {
        statisticsEntry = statsEntry;
    }

    @Override
    public byte[] getByteArray() {
        if (binaryStats.getLength() == 0) {
            int binarySize = Integer.BYTES + statisticsEntry.getLength();
            binaryStats.setSize(binarySize);
            byte[] statsByteArray = binaryStats.getByteArray();

            int offset = 0;
            IntegerPointable.setInteger(statsByteArray, offset, statisticsEntry.getLength());
            offset += Integer.BYTES;
            System.arraycopy(statisticsEntry.getByteArray(), 0, statsByteArray, offset, statisticsEntry.getLength());

            byte[] infoByteArray = getStatisticsInfoByteArray();
            length += statsByteArray.length;
            bytes = new byte[length];
            offset = 0;
            System.arraycopy(infoByteArray, 0, bytes, offset, infoByteArray.length);
            offset += infoByteArray.length;
            System.arraycopy(statsByteArray, 0, bytes, offset, statsByteArray.length);
        }
        return bytes;
    }

    private byte[] getStatisticsInfoByteArray() {
        int binarySize = 0;
        if (dataverse != null && dataset != null && index != null) {
            binarySize += Integer.BYTES + dataverse.getBytes(StandardCharsets.UTF_8).length;
            binarySize += Integer.BYTES + dataset.getBytes(StandardCharsets.UTF_8).length;
            binarySize += Integer.BYTES + index.getBytes(StandardCharsets.UTF_8).length;

            byte[] buffer = new byte[binarySize];
            int offset = 0;

            // Store length of dataverse and the corresponding string
            IntegerPointable.setInteger(buffer, offset, dataverse.getBytes(StandardCharsets.UTF_8).length);
            offset += Integer.BYTES;
            System.arraycopy(dataverse.getBytes(StandardCharsets.UTF_8), 0, buffer, offset,
                    dataverse.getBytes(StandardCharsets.UTF_8).length);
            offset += dataverse.getBytes(StandardCharsets.UTF_8).length;

            // Store length of dataset and the corresponding string
            IntegerPointable.setInteger(buffer, offset, dataset.getBytes(StandardCharsets.UTF_8).length);
            offset += Integer.BYTES;
            System.arraycopy(dataset.getBytes(StandardCharsets.UTF_8), 0, buffer, offset,
                    dataset.getBytes(StandardCharsets.UTF_8).length);
            offset += dataset.getBytes(StandardCharsets.UTF_8).length;

            // Store length of index and the corresponding string
            IntegerPointable.setInteger(buffer, offset, index.getBytes(StandardCharsets.UTF_8).length);
            offset += Integer.BYTES;
            System.arraycopy(index.getBytes(StandardCharsets.UTF_8), 0, buffer, offset,
                    index.getBytes(StandardCharsets.UTF_8).length);
            offset += index.getBytes(StandardCharsets.UTF_8).length;

            length = offset;
            return buffer;
        }
        return new byte[0];
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        if (length == 0) {
            getByteArray();
        }
        return length;
    }

    @Override
    public void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        int offset = setStatsInfoFromByteArray(bytes, start, length);
        int byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        byte[] dummy = new byte[byteLength];
        if (offset - start + byteLength > length) {
            this.length = offset - start - Integer.BYTES;
            binaryStats.reset();
            return;
        }
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        statisticsEntry = new StatisticsEntry();
        statisticsEntry.set(dummy, 0, byteLength);
        offset += byteLength;

        this.length = offset - start;
        this.bytes = new byte[this.length];
        System.arraycopy(bytes, start, this.bytes, 0, this.length);
        binaryStats.reset();
    }

    private int setStatsInfoFromByteArray(byte[] bytes, int start, int length) {
        int offset = start;

        // dataverse name
        int byteLength = IntegerPointable.getInteger(bytes, offset);
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return start;
        }
        offset += Integer.BYTES;
        byte[] dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        dataverse = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        // dataset name
        byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return start;
        }
        dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        dataset = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        // index name
        byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return start;
        }
        dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        index = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        return offset;
    }

    private void reset() {
        dataverse = null;
        dataset = null;
        index = null;
    }
}