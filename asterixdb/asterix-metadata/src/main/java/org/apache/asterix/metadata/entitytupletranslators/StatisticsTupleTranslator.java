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
package org.apache.asterix.metadata.entitytupletranslators;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Statistics;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisElementType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis.SynopsisType;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsis;
import org.apache.hyracks.storage.am.statistics.common.SynopsisFactory;
import org.apache.hyracks.storage.am.statistics.historgram.HistogramBucket;
import org.apache.hyracks.storage.am.statistics.historgram.UniformHistogramBucket;

/**
 * Translates a Statistics metadata entity to an ITupleReference and vice versa.
 */
public class StatisticsTupleTranslator extends AbstractTupleTranslator<Statistics> {
    private static final int STATISTICS_PAYLOAD_TUPLE_FIELD_INDEX = 6;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private AMutableInt64 aInt64;
    private AMutableInt32 aInt32;
    private AMutableDouble aDouble;

    private final MetadataNode metadataNode;
    private final TxnId txnId;

    public StatisticsTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.STATISTICS_DATASET, STATISTICS_PAYLOAD_TUPLE_FIELD_INDEX);
        this.txnId = txnId;
        this.metadataNode = metadataNode;
        if (getTuple) {
            aInt32 = new AMutableInt32(0);
            aInt64 = new AMutableInt64(0);
            aDouble = new AMutableDouble(0.0);
        }
    }

    @Override
    public Statistics createMetadataEntityFromARecord(ARecord statisticsRecord) throws AlgebricksException {

        String dataverseCanonicalName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);

        String datasetName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_DATASET_NAME_FIELD_INDEX)).getStringValue();

        String indexName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_INDEX_NAME_FIELD_INDEX)).getStringValue();

        boolean isAntimatter = ((ABoolean) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_ISANTIMATTER_FIELD_INDEX)).getBoolean();

        String fieldName = ((AString) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_FIELD_NAME_FIELD_INDEX)).getStringValue();

        int synopsisElementType = ((AInt32) statisticsRecord
                .getValueByPos(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_ELEMENTS_TYPE_INDEX)).getIntegerValue();

        SynopsisElementType type = synopsisElementType == 0 ? SynopsisElementType.Long : SynopsisElementType.Double;

        // Get open fields (synopsis, numTuples, totalTuplesSize)
        AbstractSynopsis synopsis =
                getSynopsisFromARecord(statisticsRecord, dataverseName, datasetName, fieldName, type);
        long numTuples = getNumTuplesFromARecord(statisticsRecord);
        long totalTuplesSize = getTotalTuplesSizeFromARecord(statisticsRecord);

        return new Statistics(dataverseName, datasetName, indexName, false, isAntimatter, fieldName, synopsis,
                numTuples, totalTuplesSize);

    }

    private AbstractSynopsis getSynopsisFromARecord(ARecord statisticsRecord, DataverseName dataverseName,
            String datasetName, String fieldName, SynopsisElementType synopsisElementType) {
        ARecordType statisticsRecordType = statisticsRecord.getType();
        int synopsisRecordIndex =
                statisticsRecordType.getFieldIndex(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_FIELD_NAME);
        if (synopsisRecordIndex < 0) {
            return null;
        }

        ARecord synopsisRecord = (ARecord) statisticsRecord.getValueByPos(synopsisRecordIndex);
        ARecordType synopsisRecordType = (ARecordType) statisticsRecordType.getFieldTypes()[synopsisRecordIndex];

        int synopsisTypeIndex =
                synopsisRecordType.getFieldIndex(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_TYPE_FIELD_NAME);
        int synopsisSizeIndex =
                synopsisRecordType.getFieldIndex(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_SIZE_FIELD_NAME);
        SynopsisType synopsisType =
                SynopsisType.valueOf(((AString) synopsisRecord.getValueByPos(synopsisTypeIndex)).getStringValue());
        int synopsisSize = ((AInt32) synopsisRecord.getValueByPos(synopsisSizeIndex)).getIntegerValue();

        int synopsisElementsIndex =
                synopsisRecordType.getFieldIndex(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_ELEMENTS_FIELD_NAME);
        AOrderedList elementsList = (AOrderedList) synopsisRecord.getValueByPos(synopsisElementsIndex);
        IACursor cursor = elementsList.getCursor();
        List<ISynopsisElement> elems = new ArrayList<>(elementsList.size());
        while (cursor.next()) {
            ARecord coeff = (ARecord) cursor.get();
            if (synopsisType == SynopsisType.ContinuousHistogram) {
                switch (synopsisElementType) {
                    case Long:
                        elems.add(new HistogramBucket<Long>(
                                ((AInt64) coeff.getValueByPos(
                                        MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_LBORDER_FIELD_INDEX))
                                                .getLongValue(),
                                ((AInt64) coeff.getValueByPos(
                                        MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_RBORDER_FIELD_INDEX))
                                                .getLongValue(),
                                ((ADouble) coeff
                                        .getValueByPos(MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_VALUE_FIELD_INDEX))
                                                .getDoubleValue()));
                        break;
                    case Double:
                        elems.add(new HistogramBucket<Double>(
                                ((ADouble) coeff.getValueByPos(
                                        MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_LBORDER_FIELD_INDEX))
                                                .getDoubleValue(),
                                ((ADouble) coeff.getValueByPos(
                                        MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_RBORDER_FIELD_INDEX))
                                                .getDoubleValue(),
                                ((ADouble) coeff
                                        .getValueByPos(MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_VALUE_FIELD_INDEX))
                                                .getDoubleValue()));
                        break;
                }
            }
        }

        Dataset ds;
        Datatype type = null;
        try {
            ds = metadataNode.getDataset(txnId, dataverseName, datasetName);
            type = metadataNode.getDatatype(txnId, ds.getItemTypeDataverseName(), ds.getItemTypeName());
        } catch (AlgebricksException e1) {
            e1.printStackTrace();
        }
        ITypeTraits keyTypeTraits =
                TypeTraitProvider.INSTANCE.getTypeTrait(((ARecordType) type.getDatatype()).getFieldType(fieldName));

        AbstractSynopsis synopsis = null;
        try {
            if (synopsisElementType.equals(SynopsisElementType.Long)) {
                synopsis = SynopsisFactory.createSynopsis(synopsisType, keyTypeTraits, elems, elems.size(),
                        synopsisSize, ISynopsis.SynopsisElementType.Long);
            } else if (synopsisElementType.equals(SynopsisElementType.Double)) {
                synopsis = SynopsisFactory.createSynopsis(synopsisType, keyTypeTraits, elems, elems.size(),
                        synopsisSize, ISynopsis.SynopsisElementType.Double);
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        return synopsis;
    }

    private long getNumTuplesFromARecord(ARecord statisticsRecord) {
        int numTuplesIndex =
                statisticsRecord.getType().getFieldIndex(MetadataRecordTypes.STATISTICS_NUM_TUPLES_FIELD_NAME);
        return (numTuplesIndex >= 0) ? ((AInt64) statisticsRecord.getValueByPos(numTuplesIndex)).getLongValue() : 0L;
    }

    private long getTotalTuplesSizeFromARecord(ARecord statisticsRecord) {
        int tuplesSizeIndex =
                statisticsRecord.getType().getFieldIndex(MetadataRecordTypes.STATISTICS_TOTAL_TUPLES_SIZE_FIELD_NAME);
        return (tuplesSizeIndex >= 0) ? ((AInt64) statisticsRecord.getValueByPos(tuplesSizeIndex)).getLongValue() : 0L;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Statistics metadataEntity)
            throws HyracksDataException, MetadataException {
        // write the key in the first 6 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(metadataEntity.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getIndexName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        booleanSerde.serialize(metadataEntity.isAntimatter() ? ABoolean.TRUE : ABoolean.FALSE,
                tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(metadataEntity.getFieldName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aInt32.setValue(metadataEntity.getSynopsis().getElementType().getValue());
        int32Serde.serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the 7th field of the tuple
        recordBuilder.reset(MetadataRecordTypes.STATISTICS_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(metadataEntity.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(metadataEntity.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_DATASET_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(metadataEntity.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_INDEX_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        booleanSerde.serialize(metadataEntity.isAntimatter() ? ABoolean.TRUE : ABoolean.FALSE,
                fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_ISANTIMATTER_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(metadataEntity.getFieldName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_FIELD_NAME_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aInt32.setValue(metadataEntity.getSynopsis().getElementType().getValue());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_ELEMENTS_TYPE_INDEX, fieldValue);

        // write open fields (synopsis, numTuples, totalTuplesSize)
        writeSynopsisRecordType(metadataEntity);
        writeNumTuples(metadataEntity);
        writeTotalTuplesSize(metadataEntity);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeSynopsisRecordType(Statistics statistics) throws HyracksDataException {
        if (statistics.getSynopsis() == null) {
            return;
        }

        IARecordBuilder synopsisRecordBuilder = new RecordBuilder();
        IARecordBuilder synopsisElementRecordBuilder = new RecordBuilder();
        SynopsisElementType type = statistics.getSynopsis().getElementType();

        synopsisRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage listItemValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage synopsisItemValue = new ArrayBackedValueStorage();

        // write field 0
        synopsisItemValue.reset();
        aString.setValue(statistics.getSynopsis().getType().name());
        stringSerde.serialize(aString, synopsisItemValue.getDataOutput());
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_TYPE_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        synopsisRecordBuilder.addField(fieldName, synopsisItemValue);

        // write field 1
        synopsisItemValue.reset();
        aInt32.setValue(statistics.getSynopsis().getSize());
        int32Serde.serialize(aInt32, synopsisItemValue.getDataOutput());
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_SIZE_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        synopsisRecordBuilder.addField(fieldName, synopsisItemValue);

        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (ISynopsisElement<? extends Number> synopsisElement : statistics.getSynopsis().getElements()) {
            // Skip synopsis elements with 0 value
            if (synopsisElement.getValue() != 0.0) {
                listItemValue.reset();
                synopsisElementRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

                // write sub-record field 0 and 1 of field 2
                switch (type) {
                    case Long:
                        // write left border
                        synopsisItemValue.reset();
                        aInt64.setValue(synopsisElement.getLeftKey().longValue());
                        int64Serde.serialize(aInt64, synopsisItemValue.getDataOutput());
                        fieldName.reset();
                        aString.setValue(MetadataRecordTypes.FIELD_NAME_LKEY);
                        stringSerde.serialize(aString, fieldName.getDataOutput());
                        synopsisElementRecordBuilder.addField(fieldName, synopsisItemValue);

                        // write right border
                        synopsisItemValue.reset();
                        aInt64.setValue(synopsisElement.getRightKey().longValue());
                        int64Serde.serialize(aInt64, synopsisItemValue.getDataOutput());
                        fieldName.reset();
                        aString.setValue(MetadataRecordTypes.FIELD_NAME_RKEY);
                        stringSerde.serialize(aString, fieldName.getDataOutput());
                        synopsisElementRecordBuilder.addField(fieldName, synopsisItemValue);
                        break;
                    case Double:
                        // write left border
                        synopsisItemValue.reset();
                        aDouble.setValue(synopsisElement.getLeftKey().doubleValue());
                        doubleSerde.serialize(aDouble, synopsisItemValue.getDataOutput());
                        fieldName.reset();
                        aString.setValue(MetadataRecordTypes.FIELD_NAME_LKEY);
                        stringSerde.serialize(aString, fieldName.getDataOutput());
                        synopsisElementRecordBuilder.addField(fieldName, synopsisItemValue);

                        // write right border
                        synopsisItemValue.reset();
                        aDouble.setValue(synopsisElement.getRightKey().doubleValue());
                        doubleSerde.serialize(aDouble, synopsisItemValue.getDataOutput());
                        fieldName.reset();
                        aString.setValue(MetadataRecordTypes.FIELD_NAME_RKEY);
                        stringSerde.serialize(aString, fieldName.getDataOutput());
                        synopsisElementRecordBuilder.addField(fieldName, synopsisItemValue);
                        break;
                }

                // write sub-record field 2 of field 2
                synopsisItemValue.reset();
                aDouble.setValue(synopsisElement.getValue());
                doubleSerde.serialize(aDouble, synopsisItemValue.getDataOutput());
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_VALUE);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                synopsisElementRecordBuilder.addField(fieldName, synopsisItemValue);

                // write optional field 3
                if (synopsisElement instanceof UniformHistogramBucket) {
                    ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();

                    synopsisItemValue.reset();
                    nameValue.reset();
                    aString.setValue(MetadataRecordTypes.SYNOPSIS_ELEMENT_ARECORD_UNIQUE_VALUES_NUM_FIELD_NAME);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    aInt64.setValue(((UniformHistogramBucket) synopsisElement).getUniqueElementsNum());
                    int64Serde.serialize(aInt64, synopsisItemValue.getDataOutput());
                    synopsisElementRecordBuilder.addField(nameValue, synopsisItemValue);
                }

                synopsisElementRecordBuilder.write(listItemValue.getDataOutput(), true);
                listBuilder.addItem(listItemValue);
            }
        }

        // write field 2
        synopsisItemValue.reset();
        listBuilder.write(synopsisItemValue.getDataOutput(), true);
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.STATISTICS_SYNOPSIS_ARECORD_ELEMENTS_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        synopsisRecordBuilder.addField(fieldName, synopsisItemValue);

        fieldValue.reset();
        synopsisRecordBuilder.write(fieldValue.getDataOutput(), true);
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.STATISTICS_ARECORD_SYNOPSIS_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeNumTuples(Statistics statistics) throws HyracksDataException {
        if (statistics.getNumTuples() > 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.STATISTICS_NUM_TUPLES_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aInt64.setValue(statistics.getNumTuples());
            int64Serde.serialize(aInt64, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    private void writeTotalTuplesSize(Statistics statistics) throws HyracksDataException {
        if (statistics.getTotalTuplesSize() > 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.STATISTICS_TOTAL_TUPLES_SIZE_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aInt64.setValue(statistics.getTotalTuplesSize());
            int64Serde.serialize(aInt64, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }
}
