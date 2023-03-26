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
package org.apache.asterix.metadata.declared;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisElementType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.SynopsisFactory;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;

public class StatisticsEntry extends AbstractPointable implements Serializable {
    private static final long serialVersionUID = 1L;

    private ISynopsis<? extends ISynopsisElement<? extends Number>> synopsis;
    private String dataverse;
    private String dataset;
    private String index;
    private String field;

    public StatisticsEntry() {
    }

    public StatisticsEntry(ISynopsis<? extends ISynopsisElement<? extends Number>> synopsis, String dataverse,
            String dataset, String index, String field) {
        this.synopsis = synopsis;
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.index = index;
        this.field = field;
    }

    public ISynopsis<? extends ISynopsisElement<? extends Number>> getSynopsis() {
        return synopsis;
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

    public String getField() {
        return field;
    }

    private byte[] getSynopsisByteArray() {
        if (synopsis != null) {
            // PS: Long.BYTES == Double.BYTES (for integer and double datatype)
            // byte format of a synopsis,
            // [size: int] [synopsisTypeLength: int] [synopsisType: string]
            // [numOfElements: int] [synopsisElemType: int]
            // [[leftBorder: long/double] [rightBorder: long/double] [value: double]...]
            int binarySize = 0;
            String synopsisTypeName = synopsis.getType().name();
            binarySize += Integer.BYTES + Integer.BYTES + synopsisTypeName.getBytes(StandardCharsets.UTF_8).length;
            binarySize += Integer.BYTES + Integer.BYTES;
            binarySize += (Long.BYTES + Long.BYTES + Double.BYTES) * (synopsis.getElements().size());
            byte[] byteArray = new byte[binarySize];

            int offset = 0;
            IntegerPointable.setInteger(byteArray, offset, synopsis.getSize());
            offset += Integer.BYTES;
            IntegerPointable.setInteger(byteArray, offset, synopsisTypeName.getBytes(StandardCharsets.UTF_8).length);
            offset += Integer.BYTES;
            System.arraycopy(synopsisTypeName.getBytes(StandardCharsets.UTF_8), 0, byteArray, offset,
                    synopsisTypeName.getBytes(StandardCharsets.UTF_8).length);
            offset += synopsisTypeName.getBytes(StandardCharsets.UTF_8).length;

            int numOfElements = synopsis.getElements().size();
            IntegerPointable.setInteger(byteArray, offset, numOfElements);
            offset += Integer.BYTES;
            int synopsisElemType = synopsis.getElementType().getValue();
            IntegerPointable.setInteger(byteArray, offset, synopsisElemType);
            offset += Integer.BYTES;

            for (Object obj : synopsis.getElements()) {
                ISynopsisElement<Number> element = (ISynopsisElement) obj;
                if (synopsisElemType == SynopsisElementType.Long.getValue()) {
                    LongPointable.setLong(byteArray, offset, element.getLeftKey().longValue());
                    offset += Long.BYTES;
                    LongPointable.setLong(byteArray, offset, element.getRightKey().longValue());
                } else if (synopsisElemType == SynopsisElementType.Double.getValue()) {
                    DoublePointable.setDouble(byteArray, offset, element.getLeftKey().doubleValue());
                    offset += Long.BYTES;
                    DoublePointable.setDouble(byteArray, offset, element.getRightKey().doubleValue());
                }
                offset += Long.BYTES;
                DoublePointable.setDouble(byteArray, offset, element.getValue());
                offset += Double.BYTES;
            }
            return byteArray;
        }
        return new byte[0];
    }

    @Override
    public byte[] getByteArray() {
        int binarySize = 0;
        if (length == 0) {
            if (dataverse != null && dataset != null && index != null && field != null && synopsis != null) {
                binarySize += Integer.BYTES + dataverse.getBytes(StandardCharsets.UTF_8).length;
                binarySize += Integer.BYTES + dataset.getBytes(StandardCharsets.UTF_8).length;
                binarySize += Integer.BYTES + index.getBytes(StandardCharsets.UTF_8).length;
                binarySize += Integer.BYTES + field.getBytes(StandardCharsets.UTF_8).length;
                byte[] synopsisByteArray = getSynopsisByteArray();
                binarySize += Integer.BYTES + synopsisByteArray.length;

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

                // Store length of field and the corresponding string
                IntegerPointable.setInteger(buffer, offset, field.getBytes(StandardCharsets.UTF_8).length);
                offset += Integer.BYTES;
                System.arraycopy(field.getBytes(StandardCharsets.UTF_8), 0, buffer, offset,
                        field.getBytes(StandardCharsets.UTF_8).length);
                offset += field.getBytes(StandardCharsets.UTF_8).length;

                // Store length of synopsis and the corresponding byte-array
                IntegerPointable.setInteger(buffer, offset, synopsisByteArray.length);
                offset += Integer.BYTES;
                System.arraycopy(synopsisByteArray, 0, buffer, offset, synopsisByteArray.length);

                length = offset + synopsisByteArray.length;
                bytes = new byte[length];
                System.arraycopy(buffer, 0, bytes, 0, length);
                return buffer;
            }
        }
        return bytes;
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

    private void setSynopsisFromByteArray(byte[] bytes, int start, int length) {
        int offset = start;
        int synopsisSize = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        int byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        byte[] dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        String synopsisType = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        int numOfElements = IntegerPointable.getInteger(bytes, offset);
        SynopsisType type = SynopsisType.None;
        List<ISynopsisElement> elements = new ArrayList<>();
        int synopsisElemType = -1;
        if (synopsisType.equals(SynopsisType.ContinuousHistogram.name())) {
            offset += Integer.BYTES;
            synopsisElemType = IntegerPointable.getInteger(bytes, offset);
            offset += Integer.BYTES;
            for (int i = 0; i < numOfElements; i++) {
                if (offset - start + Long.BYTES + Long.BYTES + Double.BYTES > length) {
                    reset();
                    return;
                }
                if (synopsisElemType == SynopsisElementType.Long.getValue()) {
                    long leftBorder = LongPointable.getLong(bytes, offset);
                    offset += Long.BYTES;
                    long rightBorder = LongPointable.getLong(bytes, offset);
                    offset += Long.BYTES;
                    double value = DoublePointable.getDouble(bytes, offset);
                    offset += Double.BYTES;
                    elements.add(new HistogramBucket<Long>(leftBorder, rightBorder, value));
                } else if (synopsisElemType == SynopsisElementType.Double.getValue()) {
                    double leftBorder = DoublePointable.getDouble(bytes, offset);
                    offset += Long.BYTES;
                    double rightBorder = DoublePointable.getDouble(bytes, offset);
                    offset += Long.BYTES;
                    double value = DoublePointable.getDouble(bytes, offset);
                    offset += Double.BYTES;
                    elements.add(new HistogramBucket<Double>(leftBorder, rightBorder, value));
                }
            }
            type = SynopsisType.ContinuousHistogram;
        } else if (synopsisType.equals(SynopsisType.QuantileSketch.name())) {
            elements = new ArrayList<>(numOfElements);
            type = SynopsisType.QuantileSketch;
        }

        try {
            if (synopsisElemType >= 0 && synopsisElemType == SynopsisElementType.Long.getValue()) {
                // Only the type, size and elements of this synopsis are used, so type_trait is a dummy one.
                synopsis = SynopsisFactory.createSynopsis(type, IntegerPointable.TYPE_TRAITS, elements, numOfElements,
                        synopsisSize, SynopsisElementType.Long);
            } else if (synopsisElemType >= 0 && synopsisElemType == SynopsisElementType.Double.getValue()) {
                synopsis = SynopsisFactory.createSynopsis(type, DoublePointable.TYPE_TRAITS, elements, numOfElements,
                        synopsisSize, SynopsisElementType.Double);
            }
        } catch (HyracksDataException e) {
            reset();
        }
    }

    @Override
    public void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        int offset = start;

        // dataverse name
        int byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return;
        }
        byte[] dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        dataverse = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        // dataset name
        byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return;
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
            return;
        }
        dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        index = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        // field name
        byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return;
        }
        dummy = new byte[byteLength];
        System.arraycopy(bytes, offset, dummy, 0, byteLength);
        field = new String(dummy, StandardCharsets.UTF_8);
        offset += byteLength;

        // synopsis
        byteLength = IntegerPointable.getInteger(bytes, offset);
        offset += Integer.BYTES;
        if (byteLength == 0 || (offset - start + byteLength) > length) {
            reset();
            return;
        }
        setSynopsisFromByteArray(bytes, offset, byteLength);

        if (dataverse != null && dataset != null && index != null && field != null && synopsis != null) {
            this.length = offset - start + byteLength;
            this.bytes = new byte[this.length];
            System.arraycopy(bytes, 0, this.bytes, 0, this.length);
        }
    }

    /**
     * This is only for handling errors, not for general usage
     */
    private void reset() {
        dataverse = null;
        dataset = null;
        index = null;
        field = null;
        synopsis = null;
        length = 0;
        bytes = new byte[0];
    }

    public String toString() {
        return "TestStatisticsMessageID{" + "dataverse='" + dataverse + '\'' + ", dataset='" + dataset + '\''
                + ", index='" + index + '\'' + ", field='" + field + '\'' + '}';
    }
}