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
package org.apache.asterix.external.provider;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.adapter.factory.LookupAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IIndexingAdapterFactory;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.util.ExternalDataCompatibilityUtils;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;

/**
 * This class represents the entry point to all things adapters
 */
public class AdapterFactoryProvider {

    // Internal Adapters
    public static IAdapterFactory getAdapterFactory(String adapterName, Map<String, String> configuration,
            ARecordType itemType, ARecordType metaType) throws AsterixException {
        ExternalDataCompatibilityUtils.prepare(adapterName, configuration);
        ExternalDataUtils.validateParameters(configuration);
        GenericAdapterFactory adapterFactory = new GenericAdapterFactory();
        adapterFactory.configure(configuration, itemType, metaType);
        return adapterFactory;
    }

    // Indexing Adapters
    public static IIndexingAdapterFactory getIndexingAdapterFactory(String adapterName,
            Map<String, String> configuration, ARecordType itemType, List<ExternalFile> snapshot, boolean indexingOp,
            ARecordType metaType) throws AsterixException {
        ExternalDataCompatibilityUtils.prepare(adapterName, configuration);
        ExternalDataUtils.validateParameters(configuration);
        GenericAdapterFactory adapterFactory = new GenericAdapterFactory();
        adapterFactory.setSnapshot(snapshot, indexingOp);
        adapterFactory.configure(configuration, itemType, metaType);
        return adapterFactory;
    }

    // Lookup Adapters
    public static LookupAdapterFactory<?> getLookupAdapterFactory(Map<String, String> configuration,
            ARecordType recordType, int[] ridFields, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory) throws AsterixException {
        LookupAdapterFactory<?> adapterFactory = new LookupAdapterFactory<>(recordType, ridFields, retainInput,
                retainMissing, missingWriterFactory);
        adapterFactory.configure(configuration);
        return adapterFactory;
    }
}
