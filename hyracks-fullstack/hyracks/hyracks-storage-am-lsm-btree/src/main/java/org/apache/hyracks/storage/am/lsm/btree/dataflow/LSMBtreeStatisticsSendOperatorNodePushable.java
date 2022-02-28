package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMBtreeStatisticsSendOperatorNodePushable extends AbstractOperatorNodePushable {
    private final IIndexDataflowHelper indexHelper;

    public LSMBtreeStatisticsSendOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory) throws HyracksDataException{
        indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
    }

    @Override
    public void initialize() throws HyracksDataException {
        indexHelper.open();
        ILSMIndex index = (ILSMIndex) indexHelper.getIndexInstance();
        ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.sendDiskComponentsStatistics();
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        indexHelper.close();
    }

    @Override
    public int getInputArity() {
        return 0;
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {

    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }
}
