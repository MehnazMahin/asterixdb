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
package org.apache.asterix.lang.common.statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class UpdateStatisticsStatement extends AbstractStatement {
    private DataverseName dataverseName;
    private String datasetName;
    private List<String> indexNames = new ArrayList<>();

    public UpdateStatisticsStatement() {
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(DataverseName dataverseName) {
        this.dataverseName = dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public List<String> getIndexNames() {
        return indexNames;
    }

    public void addIndexNames(String indexName) {
        indexNames.add(indexName);
    }

    @Override
    public Kind getKind() {
        return Kind.STATISTICS_UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetName, dataverseName, indexNames);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UpdateStatisticsStatement)) {
            return false;
        }
        UpdateStatisticsStatement target = (UpdateStatisticsStatement) object;
        return Objects.equals(dataverseName, target.dataverseName) && Objects.equals(datasetName, target.datasetName)
                && Objects.equals(indexNames, target.indexNames);
    }

    // As we update the metadata with the statistics
    @Override
    public byte getCategory() {
        return Category.DDL;
    }

}