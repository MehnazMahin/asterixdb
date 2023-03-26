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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSink;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ISynopsis;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatisticsId;

public class TestMetadataProvider implements IMetadataProvider<DataSourceId, String> {

    public static class TestStatisticsID {
        private String dataverse;
        private String dataset;
        private String index;
        private String field;
        private String node;
        private String partition;
        private boolean isAntimatter;

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestStatisticsID that = (TestStatisticsID) o;
            return dataverse.equals(that.dataverse) && dataset.equals(that.dataset) && index.equals(that.index)
                    && field.equals(that.field) && node.equals(that.node) && partition.equals(that.partition)
                    && isAntimatter == that.isAntimatter;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataverse, dataset, index, field, node, partition, isAntimatter);
        }

        @Override
        public String toString() {
            return "TestStatisticsMessageID{" + "dataverse='" + dataverse + '\'' + ", dataset='" + dataset + '\''
                    + ", index='" + index + '\'' + ", field='" + field + '\'' + ", node='" + node + '\''
                    + ", partition='" + partition + '\'' + ", isAntimatter=" + isAntimatter + '}';
        }

        public TestStatisticsID(String dataverse, String dataset, String index, String field, String node,
                String partition, boolean isAntimatter) {
            this.dataverse = dataverse;
            this.dataset = dataset;
            this.index = index;
            this.field = field;
            this.node = node;
            this.partition = partition;
            this.isAntimatter = isAntimatter;
        }
    }

    public static class TestStatisticsEntry implements Comparable<TestStatisticsEntry> {

        private ComponentStatisticsId componentId;

        private ISynopsis synopsis;

        public TestStatisticsEntry(ComponentStatisticsId componentId, ISynopsis synopsis) {
            this.componentId = componentId;
            this.synopsis = synopsis;
        }

        public ComponentStatisticsId getComponentId() {
            return componentId;
        }

        public ISynopsis getSynopsis() {
            return synopsis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestStatisticsEntry that = (TestStatisticsEntry) o;
            return componentId.equals(that.componentId);
        }

        @Override
        public int hashCode() {
            return componentId.hashCode();
        }

        @Override
        public int compareTo(TestStatisticsEntry o) {
            return componentIdComparator.compare(this.componentId, o.componentId);
        }
    }

    private static Comparator<ComponentStatisticsId> componentIdComparator = (o1, o2) -> {
        int startTimestampCompare = o1.getMinTimestamp() == o2.getMinTimestamp() ? 1 : 0;
        if (startTimestampCompare != 0) {
            return startTimestampCompare;
        }
        return o1.getMaxTimestamp() == o2.getMaxTimestamp() ? 1 : 0;
    };

    private Map<TestStatisticsID, Collection<TestStatisticsEntry>> stats = new HashMap<>();

    public void clearStats() {
        stats.clear();
    }

    public Collection getStats(TestStatisticsID id) {
        return stats.getOrDefault(id, Collections.EMPTY_LIST);
    }

    @Override
    public void addStatistics(String dataverseName, String datasetName, String indexName, String node, String partition,
            ComponentStatisticsId componentId, boolean isAntimatter, String fieldName, ISynopsis synopsis)
            throws AlgebricksException {
        TestStatisticsID statsId =
                new TestStatisticsID(dataverseName, datasetName, indexName, fieldName, node, partition, isAntimatter);
        PriorityQueue<TestStatisticsEntry> entries =
                (PriorityQueue<TestStatisticsEntry>) stats.computeIfAbsent(statsId, (k) -> new PriorityQueue<>());
        entries.add(new TestStatisticsEntry(componentId, synopsis));

    }

    @Override
    public void updateStatistics(String dataverseName, String datasetName, String indexName, String node,
            String partition, ComponentStatisticsId componentId, boolean isAntimatter, String fieldName,
            ISynopsis synopsis) throws AlgebricksException {
        TestStatisticsID statsId =
                new TestStatisticsID(dataverseName, datasetName, indexName, fieldName, node, partition, isAntimatter);
        PriorityQueue<TestStatisticsEntry> entries =
                (PriorityQueue<TestStatisticsEntry>) stats.computeIfAbsent(statsId, (k) -> new PriorityQueue<>());
        entries.add(new TestStatisticsEntry(componentId, synopsis));

    }

    @Override
    public void dropStatistics(String dataverseName, String datasetName, String indexName, String node,
            String partition, boolean isAntimatter, String fieldName) throws AlgebricksException {
        TestStatisticsID statsId =
                new TestStatisticsID(dataverseName, datasetName, indexName, fieldName, node, partition, isAntimatter);
        PriorityQueue<TestStatisticsEntry> entries =
                (PriorityQueue<TestStatisticsEntry>) stats.computeIfAbsent(statsId, (k) -> new PriorityQueue<>());
        Iterator<TestStatisticsEntry> eIt = entries.iterator();
        TestStatisticsEntry currEntry = null;

        while (currEntry == null && eIt.hasNext()) {
            currEntry = eIt.next();
        }
        if (currEntry != null) {
            eIt.remove();
        }
    }

    //    @Override
    //    public void updateStatistics(String dataverseName, String datasetName, String indexName, String node,
    //            String partition, ComponentStatisticsId componentId, String fieldName, boolean isAntimatter, ISynopsis synopsis)
    //            throws AlgebricksException {
    //        TestStatisticsID statsId =
    //                new TestStatisticsID(dataverseName, datasetName, indexName, fieldName, node, partition, isAntimatter);
    //        PriorityQueue<TestStatisticsEntry> entries =
    //                (PriorityQueue<TestStatisticsEntry>) stats.computeIfAbsent(statsId, (k) -> new PriorityQueue<>());
    //        Iterator<TestStatisticsEntry> eIt = entries.iterator();
    //        TestStatisticsEntry currEntry = null;
    //
    //        List<TestStatisticsEntry> statisticsEntries = new ArrayList<>();
    //        while (currEntry == null || eIt.hasNext()) {
    //            currEntry = eIt.next();
    //            if (componentId.getMinTimestamp() <= currEntry.componentId.getMinTimestamp()
    //                    && currEntry.componentId.getMaxTimestamp() <= componentId.getMaxTimestamp()) {
    //                statisticsEntries.add(currEntry);
    //            }
    //        }
    //        for (TestStatisticsEntry entry : statisticsEntries) {
    //            entries.remove(entry);
    //        }
    //        entries.add(new TestStatisticsEntry(componentId, synopsis));
    //    }

    @Override
    public IDataSource<DataSourceId> findDataSource(DataSourceId id) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<DataSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars, ITupleFilterFactory tupleFilterFactory, long outputLimit,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec,
            Object implConfig, IProjectionInfo<?> projectionInfo) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, IAWriterFactory writerFactory,
            RecordDescriptor inputDesc) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, IAWriterFactory writerFactory,
            IResultSerializerFactoryProvider resultSerializerFactoryProvider, RecordDescriptor inputDesc,
            IResultMetadata metadata, JobSpecification spec) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payLoadVar, List<LogicalVariable> additionalFilterKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification jobSpec, boolean bulkload) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            boolean bulkload, List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, DataSourceId> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalNonKeyFields,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(
            IDataSourceIndex<String, DataSourceId> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IDataSourceIndex<String, DataSourceId> findDataSourceIndex(String indexId, DataSourceId dataSourceId)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getUpsertRuntime(
            IDataSource<DataSourceId> dataSource, IOperatorSchema inputSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payLoadVar, List<LogicalVariable> additionalFilterFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexUpsertRuntime(
            IDataSourceIndex<String, DataSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, List<LogicalVariable> additionalFilteringKeys,
            ILogicalExpression filterExpr, ILogicalExpression prevFilterExpr, LogicalVariable upsertIndicatorVar,
            List<LogicalVariable> prevSecondaryKeys, LogicalVariable prevAdditionalFilteringKeys,
            RecordDescriptor inputDesc, JobGenContext context, JobSpecification spec,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines) throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlockingOperatorDisabled() {
        return false;
    }

    @Override
    public ITupleFilterFactory createTupleFilterFactory(IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, ILogicalExpression filterExpr, JobGenContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }
}
