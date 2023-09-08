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

package org.apache.asterix.optimizer.rules.cbo;

import java.util.List;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

public class DistinctCardinalityEstimation {
    private final IOptimizationContext optCtx;
    private final JoinEnum joinEnum;
    private final Stats stats;
    private long totalSamples;
    private double distinctFromSamples;

    public DistinctCardinalityEstimation(IOptimizationContext context, JoinEnum joinE) {
        optCtx = context;
        joinEnum = joinE;
        stats = joinEnum.getStatsHandle();
    }

    public void setTotalSamples(long numSamples) {
        totalSamples = numSamples;
    }

    public void setDistinctFromSamples(double distinctSamples) {
        distinctFromSamples = distinctSamples;
    }

    public long findDistinctCardinality(ILogicalOperator grpByDistinctOp) throws AlgebricksException {
        if (stats == null) {
            return 0L; // stats object is not initialized yet
        }
        long distinctCard = 0L;
        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();

        // distinct cardinality supported only for GroupByOp and DistinctOp
        if (tag == LogicalOperatorTag.DISTINCT || tag == LogicalOperatorTag.GROUP) {
            ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(grpByDistinctOp);
            DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();
            if (scanOp == null) {
                return distinctCard; // this may happen in case of in lists
            }

            Index index = stats.findSampleIndex(scanOp, optCtx);
            if (index == null) {
                return distinctCard;
            }

            Index.SampleIndexDetails idxDetails = (Index.SampleIndexDetails) index.getIndexDetails();
            double origDatasetCard = idxDetails.getSourceCardinality();

            byte dsType = ((DataSource) scanOp.getDataSource()).getDatasourceType();
            if (!(dsType == DataSource.Type.INTERNAL_DATASET || dsType == DataSource.Type.EXTERNAL_DATASET)) {
                return distinctCard; // Datasource must be of a dataset, not supported for other datasource types
            }
            SampleDataSource sampleDataSource = joinEnum.getSampleDataSource(scanOp);
            String viewInPlan = new ALogicalPlanImpl(new MutableObject<>(grpByDistinctOp)).toString(); //useful when debugging

            ILogicalOperator parentOfSelectOp = findParentOfSelectOp(grpByDistinctOp);
            SelectOperator selOp = (parentOfSelectOp == null) ? null
                    : ((SelectOperator) parentOfSelectOp.getInputs().get(0).getValue());

            setTotalSamples(idxDetails.getSampleCardinalityTarget()); // sample size without predicates (i.e., n)
            if (selOp != null) {
                long sampleWithPredicates = findSampleSizeWithPredicates(selOp, sampleDataSource);
                // set totalSamples to the sample size with predicates (i.e., n_f)
                setTotalSamples(sampleWithPredicates);
            }
            // get the estimated distinct cardinality for the dataset (i.e., D_est or D_est_f)
            distinctCard = findEstDistinctWithPredicates(grpByDistinctOp, origDatasetCard, sampleDataSource);

            /*// only for the verification purpose of the estimator accuracy
            if (selOp != null) { // get distinct estimation without predicates (i.e., D_est, for verification of accuracy)
                setTotalSamples(idxDetails.getSampleCardinalityTarget());
                long initCard = findEstDistinctWithoutPredicates(grpByDistinctOp, origDatasetCard, sampleDataSource);
            }*/
        }
        return distinctCard;
    }

    private long findSampleSizeWithPredicates(SelectOperator selOp, SampleDataSource sampleDataSource)
            throws AlgebricksException {
        long sampleSize = Long.MAX_VALUE;
        ILogicalOperator copyOfSelOp = OperatorManipulationUtil.bottomUpCopyOperators(selOp);
        if (setSampleDataSource(copyOfSelOp, sampleDataSource)) {
            List<List<IAObject>> result = stats.runSamplingQuery(optCtx, copyOfSelOp);
            sampleSize = ((AInt64) result.get(0).get(0)).getLongValue();
        }
        return sampleSize;
    }

    private long findEstDistinctWithPredicates(ILogicalOperator grpByDistinctOp, double origDatasetCardinality,
            SampleDataSource sampleDataSource) throws AlgebricksException {
        double estCardinality = -1.0;
        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();
        if (tag == LogicalOperatorTag.GROUP || tag == LogicalOperatorTag.DISTINCT) {
            ILogicalOperator copyOfGrpByDistinctOp = OperatorManipulationUtil.bottomUpCopyOperators(grpByDistinctOp);
            if (setSampleDataSource(copyOfGrpByDistinctOp, sampleDataSource)) {
                // get distinct cardinality from the sampling source
                List<List<IAObject>> result = stats.runSamplingQuery(optCtx, copyOfGrpByDistinctOp);
                estCardinality = ((double) ((AInt64) result.get(0).get(0)).getLongValue());
            }
        }
        if (estCardinality != -1.0) { // estimate distinct cardinality for the dataset from the sampled cardinality
            estCardinality = distinctEstimator(estCardinality, origDatasetCardinality);
        }
        estCardinality = Math.max(0.0, estCardinality);
        return Math.round(estCardinality);
    }

    private long findEstDistinctWithoutPredicates(ILogicalOperator grpByDistinctOp, double origDatasetCardinality,
            SampleDataSource sampleDataSource) throws AlgebricksException {
        double estCardinality = -1.0;
        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();
        if (tag == LogicalOperatorTag.GROUP || tag == LogicalOperatorTag.DISTINCT) {
            ILogicalOperator parentOfSelOp = findParentOfSelectOp(grpByDistinctOp);
            ILogicalOperator nextOp = (parentOfSelOp == null) ? null : (parentOfSelOp.getInputs().get(0).getValue());

            if (nextOp != null && nextOp.getOperatorTag() == LogicalOperatorTag.SELECT) { // skip the SelectOp
                nextOp = nextOp.getInputs().get(0).getValue(); // must be an AssignOp or DataSourceScanOp

                ILogicalOperator copyOfGrpByDistinctOp =
                        OperatorManipulationUtil.bottomUpCopyOperators(grpByDistinctOp);
                parentOfSelOp = findParentOfSelectOp(copyOfGrpByDistinctOp);
                ILogicalOperator copyOfNextOp = OperatorManipulationUtil.bottomUpCopyOperators(nextOp);
                assert parentOfSelOp != null;
                parentOfSelOp.getInputs().get(0).setValue(copyOfNextOp);

                if (setSampleDataSource(copyOfGrpByDistinctOp, sampleDataSource)) {
                    // get distinct cardinality from the sampling source
                    List<List<IAObject>> result = stats.runSamplingQuery(optCtx, copyOfGrpByDistinctOp);
                    estCardinality = ((double) ((AInt64) result.get(0).get(0)).getLongValue());
                }
            }
        }
        if (estCardinality != -1.0) { // estimate distinct cardinality for the dataset from the sampled cardinality
            estCardinality = distinctEstimator(estCardinality, origDatasetCardinality);
        }
        estCardinality = Math.max(0.0, estCardinality);
        return Math.round(estCardinality);
    }

    private double distinctEstimator(double estCardinality, double origDatasetCardinality) {
        double initCard = estCardinality; // initial number of distinct values from samples
        if (totalSamples <= 1) {
            totalSamples += 2;
            estCardinality = totalSamples - 1;
        } else if (estCardinality == totalSamples) {
            estCardinality--;
        }
        setDistinctFromSamples(estCardinality);

        int itr_counter = 0, max_counter = 1000; // allow a maximum number of iterations
        double denominator = derivativeFunctionForMMO(estCardinality);
        if (denominator == 0.0) { // Newton-Raphson method requires it to be non-zero
            return estCardinality;
        }
        double fraction = functionForMMO(estCardinality) / denominator;
        while (Math.abs(fraction) >= 0.001 && itr_counter < max_counter) {
            denominator = derivativeFunctionForMMO(estCardinality);
            if (denominator == 0.0) {
                break;
            }
            fraction = functionForMMO(estCardinality) / denominator;
            estCardinality = estCardinality - fraction;
            itr_counter++;
            if (estCardinality > origDatasetCardinality) {
                estCardinality = origDatasetCardinality; // for preventing infinite growth beyond N
                break;
            }
        }
        if (initCard > estCardinality) { // estimated cardinality cannot be less the initial one from samples
            estCardinality = initCard;
        }
        return estCardinality;
    }

    private double functionForMMO(double x) {
        return (x * (1.0 - Math.exp(-1.0 * (double) totalSamples / x)) - distinctFromSamples);
    }

    private double derivativeFunctionForMMO(double x) {
        double arg = ((double) totalSamples / x);
        return (1.0 - (arg + 1.0) * Math.exp(-1.0 * arg));
    }

    private boolean setSampleDataSource(ILogicalOperator op, SampleDataSource sampleDataSource) {
        ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(op);
        DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();
        if (scanOp == null) {
            return false;
        }
        // replace the DataSourceScanOp with the sampling source
        scanOp.setDataSource(sampleDataSource);
        return true;
    }

    private ILogicalOperator findParentOfSelectOp(ILogicalOperator op) {
        ILogicalOperator parent = null;
        LogicalOperatorTag tag = op.getOperatorTag();
        while (tag != LogicalOperatorTag.DATASOURCESCAN) {
            if (tag == LogicalOperatorTag.SELECT) {
                return parent;
            }
            parent = op;
            op = op.getInputs().get(0).getValue();
            tag = op.getOperatorTag();
        }
        return null; // no SelectOp in the query tree
    }
}
