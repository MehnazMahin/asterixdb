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
package org.apache.hyracks.storage.am.statistics.common;

import java.util.List;
import java.util.Objects;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public abstract class AbstractSynopsis<T extends ISynopsisElement<Number>> implements ISynopsis<T> {

    protected static final long serialVersionUID = 1L;

    protected final Number domainEnd;
    protected final Number domainStart;
    protected final int size;

    protected List<T> synopsisElements;

    public AbstractSynopsis(Number domainStart, Number domainEnd, int size, List<T> synopsisElements) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
        this.size = size;
        this.synopsisElements = synopsisElements;
    }

    public Number getDomainEnd() {
        return domainEnd;
    }

    public Number getDomainStart() {
        return domainStart;
    }

    @Override
    public int getSize() {
        return size;
    }

    public List<T> getElements() {
        return synopsisElements;
    }

    public void merge(List<ISynopsis<T>> synopsisList) throws HyracksDataException {
        for (ISynopsis<T> synopsis : synopsisList) {
            merge(synopsis);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AbstractSynopsis<?> that = (AbstractSynopsis<?>) o;
        if (that.getDomainStart().equals(getDomainStart()) && that.getDomainEnd().equals(getDomainEnd())) {
            if (that.getElements() != null && synopsisElements != null) {
                return that.getElements().size() == synopsisElements.size();
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(synopsisElements);
    }
}