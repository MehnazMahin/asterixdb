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

import java.util.Collection;
import java.util.List;
import java.util.Objects;

<<<<<<< HEAD
import org.apache.commons.collections4.CollectionUtils;
=======
>>>>>>> Initial commit for stats framework
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public abstract class AbstractSynopsis<T extends ISynopsisElement> implements ISynopsis<T> {

    protected static final long serialVersionUID = 1L;

    protected final long domainEnd;
    protected final long domainStart;
<<<<<<< HEAD
    protected final int maxLevel;
=======
>>>>>>> Initial commit for stats framework
    protected final int size;

    protected Collection<T> synopsisElements;

<<<<<<< HEAD
    public AbstractSynopsis(long domainStart, long domainEnd, int maxLevel, int size, Collection<T> synopsisElements) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
        this.maxLevel = maxLevel;
=======
    public AbstractSynopsis(long domainStart, long domainEnd, int size, Collection<T> synopsisElements) {
        this.domainStart = domainStart;
        this.domainEnd = domainEnd;
>>>>>>> Initial commit for stats framework
        this.size = size;
        this.synopsisElements = synopsisElements;
    }

    public long getDomainEnd() {
        return domainEnd;
    }

    public long getDomainStart() {
        return domainStart;
    }

<<<<<<< HEAD
    public int getMaxLevel() {
        return maxLevel;
    }

=======
>>>>>>> Initial commit for stats framework
    @Override
    public int getSize() {
        return size;
    }

    public Collection<T> getElements() {
        return synopsisElements;
    }

    public void merge(List<ISynopsis<T>> synopsisList) throws HyracksDataException {
        for (ISynopsis<T> synopsis : synopsisList) {
            merge(synopsis);
        }
    }

    public abstract double pointQuery(long position);

    public abstract double rangeQuery(long startPosition, long endPosition);

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AbstractSynopsis<?> that = (AbstractSynopsis<?>) o;
<<<<<<< HEAD
        return CollectionUtils.isEqualCollection(synopsisElements, that.synopsisElements);
=======
        if (that.getDomainStart() == getDomainStart() && that.getDomainEnd() == getDomainEnd()) {
            if (that.getElements() != null && synopsisElements != null) {
                return that.getElements().size() == synopsisElements.size();
            }
        }
        return false;
>>>>>>> Initial commit for stats framework
    }

    @Override
    public int hashCode() {
        return Objects.hash(synopsisElements);
    }
}
