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
package org.apache.asterix.runtime.statistics;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.storage.am.statistics.common.IFieldExtractor;

public class StatisticsUtil {
    private StatisticsUtil() {
    }

    /**
     * Note: Statistics collection support only
     * (1) indexed fields (non-composite field) of secondary indexes and
     * (2) primary index field iff primary keys are enabled in Query-defined config
     */
    public static List<IFieldExtractor> computeStatisticsFieldExtractors(
            IStorageComponentProvider storageComponentProvider, ARecordType recordType, List<List<String>> indexKeys,
            boolean isPrimaryIndex, boolean keepStatisticsOnPrimaryKeys) throws AlgebricksException {
        if (indexKeys.size() > 1) {
            throw new AsterixException("Cannot collect statistics on composite fields");
        }
        List<IFieldExtractor> result = new ArrayList<>();
        // TODO: allow nested fields
        if (indexKeys.isEmpty()) {
            return result;
        }
        String keyField = String.join(".", indexKeys.get(0));
        IAType keyType = recordType.getFieldType(keyField);
        ITypeTraitProvider typeTraitProvider = storageComponentProvider.getTypeTraitProvider();
        // add statistics on indexed fields
        if ((!isPrimaryIndex || keepStatisticsOnPrimaryKeys)
                && ATypeHierarchy.getTypeDomain(keyType.getTypeTag()) == Domain.NUMERIC) {
            ISerializerDeserializer serDe =
                    SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(keyType);
            result.add(new FieldExtractor(serDe, 0, keyField, typeTraitProvider.getTypeTrait(keyType),
                    keyType.getTypeTag()));
        }
        return result;
    }
}
