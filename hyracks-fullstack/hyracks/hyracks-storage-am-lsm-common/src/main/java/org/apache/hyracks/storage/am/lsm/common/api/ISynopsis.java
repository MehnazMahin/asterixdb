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
package org.apache.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface ISynopsis<T extends ISynopsisElement> extends Serializable {

    enum SynopsisType {
        None(false, false, false),
        ContinuousHistogram(false, false, true),
        QuantileSketch(true, false, false);

        private final boolean isMergeable;
        private final boolean isJoinable;
        private final boolean needsSortedOrder;

        SynopsisType(boolean isMergeable, boolean isJoinable, boolean needsSortedOrder) {
            this.isMergeable = isMergeable;
            this.isJoinable = isJoinable;
            this.needsSortedOrder = needsSortedOrder;
        }

        public boolean isMergeable() {
            return isMergeable;
        }

        public boolean isJoinable() {
            return isJoinable;
        }

        public boolean needsSortedOrder() {
            return needsSortedOrder;
        }
    }

    enum SynopsisElementType {
        Long(0),
        Double(1);

        private final int value;

        SynopsisElementType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    SynopsisType getType();

    SynopsisElementType getElementType();

    int getSize();

    List<T> getElements();

    /**
     * Updates the current synopses by merging it with 'mergedSynopsis'
     */
    void merge(ISynopsis<T> mergedSynopsis) throws HyracksDataException;
}