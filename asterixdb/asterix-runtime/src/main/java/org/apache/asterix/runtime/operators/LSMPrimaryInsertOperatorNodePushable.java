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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.dataflow.NoOpFrameOperationCallbackFactory;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.PrimaryIndexLogMarkerCallback;
import org.apache.asterix.transaction.management.opcallbacks.LockThenSearchOperationCallback;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameTupleProcessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class LSMPrimaryInsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private final IIndexDataflowHelper[] keyIndexHelpers;
    private MultiComparator keySearchCmp;
    private RangePredicate searchPred;
    private final IIndexCursor[] cursors;
    private final LockThenSearchOperationCallback[] searchCallbacks;
    private final ISearchOperationCallbackFactory searchCallbackFactory;
    private final IFrameTupleProcessor[] processors;
    private final LSMTreeIndexAccessor[] lsmAccessorForKeyIndexes;
    private final LSMTreeIndexAccessor[] lsmAccessorForUniqunessChecks;

    private final IFrameOperationCallback[] frameOpCallbacks;
    private boolean flushedPartialTuples;
    private int currentTupleIdx;
    private int lastFlushedTupleIdx;

    private final PermutingFrameTupleReference keyTuple;
    private final Int2ObjectMap<IntSet> partition2TuplesMap = new Int2ObjectOpenHashMap<>();
    private final SourceLocation sourceLoc;

    public LSMPrimaryInsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IIndexDataflowHelperFactory keyIndexHelperFactory,
            int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IModificationOperationCallbackFactory modCallbackFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int numOfPrimaryKeys, int[] filterFields,
            SourceLocation sourceLoc, ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap)
            throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, IndexOperation.UPSERT,
                modCallbackFactory, null, tuplePartitionerFactory, partitionsMap);
        this.sourceLoc = sourceLoc;
        this.frameOpCallbacks = new IFrameOperationCallback[partitions.length];
        this.searchCallbacks = new LockThenSearchOperationCallback[partitions.length];
        this.cursors = new IIndexCursor[partitions.length];
        this.lsmAccessorForUniqunessChecks = new LSMTreeIndexAccessor[partitions.length];
        this.lsmAccessorForKeyIndexes = new LSMTreeIndexAccessor[partitions.length];
        this.keyIndexHelpers = new IIndexDataflowHelper[partitions.length];
        this.processors = new IFrameTupleProcessor[partitions.length];
        if (keyIndexHelperFactory != null) {
            for (int i = 0; i < partitions.length; i++) {
                this.keyIndexHelpers[i] =
                        keyIndexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partitions[i]);
            }
        }
        this.searchCallbackFactory = searchCallbackFactory;
        int numFilterFieds = filterFields != null ? filterFields.length : 0;
        int[] searchKeyPermutations = new int[numOfPrimaryKeys + numFilterFieds];
        for (int i = 0; i < numOfPrimaryKeys; i++) {
            searchKeyPermutations[i] = fieldPermutation[i];
        }
        if (filterFields != null) {
            for (int i = numOfPrimaryKeys; i < searchKeyPermutations.length; i++) {
                searchKeyPermutations[i] = filterFields[i - numOfPrimaryKeys];
            }
        }
        keyTuple = new PermutingFrameTupleReference(searchKeyPermutations);
    }

    protected void beforeModification(ITupleReference tuple) {
        // this is used for extensions to modify tuples before persistence
        // do nothing in the master branch
    }

    protected void createTupleProcessors(SourceLocation sourceLoc) {
        for (int i = 0; i < partitions.length; i++) {
            LSMTreeIndexAccessor lsmAccessorForUniqunessCheck = lsmAccessorForUniqunessChecks[i];
            IIndexCursor cursor = cursors[i];
            IIndexAccessor indexAccessor = indexAccessors[i];
            LSMTreeIndexAccessor lsmAccessorForKeyIndex = lsmAccessorForKeyIndexes[i];
            LockThenSearchOperationCallback searchCallback = searchCallbacks[i];
            processors[i] = new IFrameTupleProcessor() {
                @Override
                public void process(FrameTupleAccessor accessor, ITupleReference tuple, int index)
                        throws HyracksDataException {
                    if (index < currentTupleIdx) {
                        // already processed; skip
                        return;
                    }
                    keyTuple.reset(accessor, index);
                    searchPred.reset(keyTuple, keyTuple, true, true, keySearchCmp, keySearchCmp);
                    boolean duplicate = false;

                    lsmAccessorForUniqunessCheck.search(cursor, searchPred);
                    try {
                        if (cursor.hasNext()) {
                            // duplicate, skip
                            searchCallback.release();
                            duplicate = true;
                        }
                    } finally {
                        cursor.close();
                    }
                    if (!duplicate) {
                        beforeModification(tuple);
                        ((ILSMIndexAccessor) indexAccessor).forceUpsert(tuple);
                        if (lsmAccessorForKeyIndex != null) {
                            lsmAccessorForKeyIndex.forceUpsert(keyTuple);
                        }
                    } else {
                        // we should flush previous inserted records so that these transactions can commit
                        flushPartialFrame();
                        // feed requires this nested exception to remove duplicated tuples
                        // TODO: a better way to treat duplicates?
                        throw HyracksDataException.create(ErrorCode.ERROR_PROCESSING_TUPLE,
                                HyracksDataException.create(ErrorCode.DUPLICATE_KEY), sourceLoc, index);
                    }
                    currentTupleIdx = index + 1;
                }

                @Override
                public void start() throws HyracksDataException {
                    ((LSMTreeIndexAccessor) indexAccessor).getCtx().setOperation(IndexOperation.UPSERT);
                }

                @Override
                public void finish() throws HyracksDataException {
                    ((LSMTreeIndexAccessor) indexAccessor).getCtx().setOperation(IndexOperation.UPSERT);
                }

                @Override
                public void fail(Throwable th) {
                    // no op
                }
            };
        }
    }

    @Override
    public void open() throws HyracksDataException {
        currentTupleIdx = 0;
        lastFlushedTupleIdx = 0;
        flushedPartialTuples = false;
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        try {
            INcApplicationContext appCtx =
                    (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();

            for (int i = 0; i < partitions.length; i++) {
                IIndexDataflowHelper indexHelper = indexHelpers[i];
                indexHelper.open();
                indexes[i] = indexHelper.getIndexInstance();
                IIndex index = indexes[i];
                IIndexDataflowHelper keyIndexHelper = keyIndexHelpers[i];
                IIndex indexForUniquessCheck;
                if (keyIndexHelper != null) {
                    keyIndexHelper.open();
                    indexForUniquessCheck = keyIndexHelper.getIndexInstance();
                } else {
                    indexForUniquessCheck = index;
                }
                if (ctx.getSharedObject() != null && i == 0) {
                    PrimaryIndexLogMarkerCallback callback =
                            new PrimaryIndexLogMarkerCallback((AbstractLSMIndex) indexes[0]);
                    TaskUtil.put(ILogMarkerCallback.KEY_MARKER_CALLBACK, callback, ctx);
                }
                writer.open();
                writerOpen = true;
                modCallbacks[i] =
                        modOpCallbackFactory.createModificationOperationCallback(indexHelper.getResource(), ctx, this);
                searchCallbacks[i] = (LockThenSearchOperationCallback) searchCallbackFactory
                        .createSearchOperationCallback(indexHelper.getResource().getId(), ctx, this);
                IIndexAccessParameters iap = new IndexAccessParameters(modCallbacks[i], NoOpOperationCallback.INSTANCE);
                indexAccessors[i] = index.createAccessor(iap);
                if (keyIndexHelper != null) {
                    lsmAccessorForKeyIndexes[i] = (LSMTreeIndexAccessor) indexForUniquessCheck.createAccessor(iap);
                }
                frameOpCallbacks[i] = NoOpFrameOperationCallbackFactory.INSTANCE.createFrameOperationCallback(ctx,
                        (ILSMIndexAccessor) indexAccessors[i]);
                frameOpCallbacks[i].open();
                IIndexAccessParameters iapForUniquenessCheck =
                        new IndexAccessParameters(NoOpOperationCallback.INSTANCE, searchCallbacks[i]);
                lsmAccessorForUniqunessChecks[i] =
                        (LSMTreeIndexAccessor) indexForUniquessCheck.createAccessor(iapForUniquenessCheck);
                cursors[i] = lsmAccessorForUniqunessChecks[i].createSearchCursor(false);
                LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) index,
                        appCtx.getTransactionSubsystem().getLogManager());
            }
            createTupleProcessors(sourceLoc);
            keySearchCmp =
                    BTreeUtils.getSearchMultiComparator(((ITreeIndex) indexes[0]).getComparatorFactories(), frameTuple);
            searchPred = new RangePredicate(frameTuple, frameTuple, true, true, keySearchCmp, keySearchCmp, null, null);
            appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
            frameTuple = new FrameTupleReference();
        } catch (Throwable e) { // NOSONAR: Re-thrown
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        partition2TuplesMap.clear();
        int itemCount = accessor.getTupleCount();
        for (int i = 0; i < itemCount; i++) {
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int pIdx = storagePartitionId2Index.get(storagePartition);
            IntSet tupleIndexes = partition2TuplesMap.computeIfAbsent(pIdx, k -> new IntOpenHashSet());
            tupleIndexes.add(i);
        }
        for (Int2ObjectMap.Entry<IntSet> p2tuplesMapEntry : partition2TuplesMap.int2ObjectEntrySet()) {
            int pIdx = p2tuplesMapEntry.getIntKey();
            LSMTreeIndexAccessor lsmAccessor = (LSMTreeIndexAccessor) indexAccessors[pIdx];
            IFrameOperationCallback frameOpCallback = frameOpCallbacks[pIdx];
            IFrameTupleProcessor processor = processors[pIdx];
            lsmAccessor.batchOperate(accessor, tuple, processor, frameOpCallback, p2tuplesMapEntry.getValue());
        }

        writeBuffer.ensureFrameSize(buffer.capacity());
        if (flushedPartialTuples) {
            flushPartialFrame();
        } else {
            FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
            FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
        }
        currentTupleIdx = 0;
        lastFlushedTupleIdx = 0;
        flushedPartialTuples = false;
    }

    /**
     * flushes tuples in a frame from lastFlushedTupleIdx(inclusive) to currentTupleIdx(exclusive)
     */
    @Override
    public void flushPartialFrame() throws HyracksDataException {
        if (lastFlushedTupleIdx == currentTupleIdx) {
            //nothing to flush
            return;
        }
        for (int i = lastFlushedTupleIdx; i < currentTupleIdx; i++) {
            FrameUtils.appendToWriter(writer, appender, accessor, i);
        }
        appender.write(writer, true);
        lastFlushedTupleIdx = currentTupleIdx;
        flushedPartialTuples = true;
    }

    @Override
    public void close() throws HyracksDataException {
        Throwable failure = CleanupUtils.destroy(null, cursors);
        failure = CleanupUtils.close(writer, failure);
        failure = CleanupUtils.close(indexHelpers, failure);
        failure = CleanupUtils.close(keyIndexHelpers, failure);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        // No op since nextFrame flushes by default
    }
}
