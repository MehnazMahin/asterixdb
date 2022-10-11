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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class UniformHistogramBucket extends HistogramBucket {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("rawtypes")
    public static final ISerializerDeserializer UNIQUE_VALUE_NUM_SERDE = Integer64SerializerDeserializer.INSTANCE;

    private long uniqueElementsNum;

    public UniformHistogramBucket(long border, double value, long uniqueElementsNum) {
        super(border, value);
        this.uniqueElementsNum = uniqueElementsNum;
    }

    public void incUniqueElementsNum() {
        uniqueElementsNum++;
    }

    public long getUniqueElementsNum() {
        return uniqueElementsNum;
    }

}
