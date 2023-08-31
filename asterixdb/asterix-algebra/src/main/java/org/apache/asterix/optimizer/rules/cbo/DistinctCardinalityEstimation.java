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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public class DistinctCardinalityEstimation {
    public enum DistinctEstimatorType {
        MMO,
        GEE,
        NONE
    }

    private final IOptimizationContext optCtx;
    private final JoinEnum joinEnum;
    private final Stats stats;
    private int totalSamples;
    private double distinctFromSamples;
    private DistinctEstimatorType distinctEstimatorType;

    public DistinctCardinalityEstimation(IOptimizationContext context, JoinEnum joinE, Stats stats) {
        optCtx = context;
        joinEnum = joinE;
        this.stats = stats;
    }

    public void setTotalSamples(int numSamples) {
        totalSamples = numSamples;
    }

    public void setDistinctFromSamples(double distinctSamples) {
        distinctFromSamples = distinctSamples;
    }

    public void setDistinctEstimatorType(DistinctEstimatorType type) {
        distinctEstimatorType = type;
    }

    public double findDistinctCardinality(ILogicalOperator grpByDistinctOp) throws AlgebricksException {
        double distinctCard = 0.0;
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
            setTotalSamples(idxDetails.getSampleCardinalityTarget());
            setDistinctEstimatorType(DistinctEstimatorType.MMO);

            byte dsType = ((DataSource) scanOp.getDataSource()).getDatasourceType();
            if (!(dsType == DataSource.Type.INTERNAL_DATASET || dsType == DataSource.Type.EXTERNAL_DATASET)) {
                return distinctCard; // Datasource must be of a dataset, not supported for other datasource types
            }
            SampleDataSource sampleDataSource = joinEnum.getSampleDataSource(scanOp);
            String viewInPlan = new ALogicalPlanImpl(new MutableObject<>(grpByDistinctOp)).toString(); //useful when debugging

            double initCardinality = 0.0, cardWithPredicates, sampleWithPredicates = totalSamples;
            ILogicalOperator parentOfSelectOp = findParentOfSelectOp(grpByDistinctOp);
            SelectOperator selOp = (parentOfSelectOp == null) ? null
                    : ((SelectOperator) parentOfSelectOp.getInputs().get(0).getValue());

            if (selOp != null) {
                sampleWithPredicates = findSampleSizeWithPredicates(selOp, sampleDataSource);
                setTotalSamples((int) sampleWithPredicates);
            }

            // get the estimated distinct cardinality for the dataset (scaling up from samples)
            cardWithPredicates = findEstDistinctWithPredicates(grpByDistinctOp, origDatasetCard, sampleDataSource);
            if (sampleWithPredicates == 0.0) {
                distinctCard = cardWithPredicates;
                return distinctCard;
            }

            if (selOp != null) {
                setTotalSamples(idxDetails.getSampleCardinalityTarget());
                initCardinality = findEstDistinctWithoutPredicates(grpByDistinctOp, origDatasetCard, sampleDataSource);
                distinctCard = initCardinality;
            }

            // scale estimated distinct cardinality for range predicates, assuming "uniform distribution"
            if (distinctCard / origDatasetCard > 0.01 && selOp != null) {
                distinctCard =
                        scaleDistinctForAllPredicates(selOp, distinctCard, origDatasetCard, sampleDataSource, false);
                if (cardWithPredicates / distinctCard < 0.1 && initCardinality != 0.0) {
                    distinctCard = initCardinality;
                    distinctCard =
                            scaleDistinctForAllPredicates(selOp, distinctCard, origDatasetCard, sampleDataSource, true);
                }
            } else {
                // this happens with very low number of distinct values (e.g. gender, status, etc.),
                // or without predicates
                distinctCard = cardWithPredicates;
            }
        }
        return distinctCard;
    }

    private double findSampleSizeWithPredicates(SelectOperator selOp, SampleDataSource sampleDataSource)
            throws AlgebricksException {
        double sampleSize = Double.MAX_VALUE;
        ILogicalOperator copyOfSelOp = OperatorManipulationUtil.bottomUpCopyOperators(selOp);
        if (setSampleDataSource(copyOfSelOp, sampleDataSource)) {
            List<List<IAObject>> result = stats.runSamplingQuery(optCtx, copyOfSelOp);
            sampleSize = ((double) ((AInt64) result.get(0).get(0)).getLongValue());
        }
        return sampleSize;
    }

    private double findEstDistinctWithPredicates(ILogicalOperator grpByDistinctOp, double origDatasetCardinality,
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
        return estCardinality;
    }

    private double findEstDistinctWithoutPredicates(ILogicalOperator grpByDistinctOp, double origDatasetCardinality,
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
                String viewInPlan = new ALogicalPlanImpl(new MutableObject<>(copyOfGrpByDistinctOp)).toString(); //useful when debugging

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
        return estCardinality;
    }

    private double scaleDistinctForAllPredicates(SelectOperator selOp, double estCardinality, double origDatasetCard,
            SampleDataSource sampleDataSource, boolean needFullRange) throws AlgebricksException {
        double scaledEstCardinality = estCardinality;
        ILogicalExpression selExpr = selOp.getCondition().getValue();
        if (selExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) selExpr;
            scaledEstCardinality *=
                    scaleDistinctForPredicate(funcExpr, selOp, origDatasetCard, sampleDataSource, needFullRange);
        }
        return scaledEstCardinality;
    }

    private double scaleDistinctForPredicate(AbstractFunctionCallExpression funcExpr, SelectOperator selOp,
            double origDatasetCard, SampleDataSource sampleDataSource, boolean needFullRange)
            throws AlgebricksException {
        double scaleRatio = 1.0;
        FunctionIdentifier fId = funcExpr.getFunctionIdentifier();
        if (fId == BuiltinFunctions.AND) {
            for (Mutable<ILogicalExpression> argExprRef : funcExpr.getArguments()) {
                AbstractFunctionCallExpression argExpr = (AbstractFunctionCallExpression) argExprRef.getValue();
                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    scaleRatio *=
                            scaleDistinctForPredicate(argExpr, selOp, origDatasetCard, sampleDataSource, needFullRange);
                }
            }
            return scaleRatio;
        } else if (fId == BuiltinFunctions.OR) {
            for (Mutable<ILogicalExpression> argExprRef : funcExpr.getArguments()) {
                AbstractFunctionCallExpression argExpr = (AbstractFunctionCallExpression) argExprRef.getValue();
                if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    if (argExpr.getFunctionIdentifier() == BuiltinFunctions.AND) {
                        scaleRatio *= scaleDistinctForPredicate(argExpr, selOp, origDatasetCard, sampleDataSource,
                                needFullRange);
                    }
                }
            }
            return scaleRatio;
        } else if (fId == BuiltinFunctions.NOT) {
            AbstractFunctionCallExpression argExpr =
                    (AbstractFunctionCallExpression) funcExpr.getArguments().get(0).getValue();
            FunctionIdentifier fid = argExpr.getFunctionIdentifier();
            if (needFullRange) {
                if (fid == BuiltinFunctions.LE || fid == BuiltinFunctions.LT) {
                    return 1.0 - scaleDistinctForPredicate(argExpr, selOp, origDatasetCard, sampleDataSource, true);
                } else if (fid == BuiltinFunctions.GE || fid == BuiltinFunctions.GT) {
                    return 1.0 - scaleDistinctForPredicate(argExpr, selOp, origDatasetCard, sampleDataSource, false);
                }
            }
            if (fid == BuiltinFunctions.EQ || fid == BuiltinFunctions.NEQ) {
                return 1.0 - scaleDistinctForPredicate(argExpr, selOp, origDatasetCard, sampleDataSource, true);
            }
            return 1.0;
        }

        // NOTE: scaling is applied only for numeric ranges (considering uniform distribution)
        // looking for numeric comparison kind functions (GE, GT, LE, LT, EQ, NEQ)
        if (fId == BuiltinFunctions.GE || fId == BuiltinFunctions.GT || fId == BuiltinFunctions.LE
                || fId == BuiltinFunctions.LT || fId == BuiltinFunctions.EQ || fId == BuiltinFunctions.NEQ) {
            SelectOperator newSelOp = createSelectOp(selOp, new MutableObject<>(funcExpr));
            if (newSelOp != null && setSampleDataSource(newSelOp, sampleDataSource)) {
                double[] minMaxValues = getMinMaxInSamples(newSelOp, false);
                double minValue = minMaxValues[0], maxValue = minMaxValues[1];

                // NOTE: scaling works only for positive numeric values
                if (minValue != Double.MAX_VALUE && maxValue != Double.MAX_VALUE) {
                    double[] rangeValues = getMinMaxInSamples(newSelOp, true);
                    double predicateRange = (maxValue - minValue + 1);
                    double denominator = Math.min(origDatasetCard, (maxValue - rangeValues[0] + 1));
                    if ((needFullRange && (fId == BuiltinFunctions.LE || fId == BuiltinFunctions.LT))
                            || fId == BuiltinFunctions.EQ || fId == BuiltinFunctions.NEQ) {
                        denominator = rangeValues[1] - rangeValues[0] + 1;
                    }
                    if (predicateRange <= denominator) {
                        scaleRatio = predicateRange / denominator;
                    }
                }
            }
        }
        return scaleRatio;
    }

    private double[] getMinMaxInSamples(SelectOperator selOp, boolean needFullRange) throws AlgebricksException {
        double[] minMaxValues = { Double.MAX_VALUE, Double.MAX_VALUE };
        ILogicalExpression expr = selOp.getCondition().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return minMaxValues;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fId = funcExpr.getFunctionIdentifier();
        if (fId != BuiltinFunctions.GT && fId != BuiltinFunctions.GE && fId != BuiltinFunctions.LT
                && fId != BuiltinFunctions.LE && fId != BuiltinFunctions.EQ && fId != BuiltinFunctions.NEQ) {
            return minMaxValues; // NOTE: scaling is applied only for numeric ranges (considering uniform distribution)
        }

        LogicalExpressionTag firstArgTag = funcExpr.getArguments().get(0).getValue().getExpressionTag();
        LogicalExpressionTag secondArgTag = funcExpr.getArguments().get(1).getValue().getExpressionTag();
        ILogicalExpression argExpr = null;
        if (secondArgTag == LogicalExpressionTag.CONSTANT) {
            if (firstArgTag == LogicalExpressionTag.VARIABLE || firstArgTag == LogicalExpressionTag.FUNCTION_CALL) {
                argExpr = funcExpr.getArguments().get(0).getValue();
            }
        } else if (firstArgTag == LogicalExpressionTag.CONSTANT) {
            if (secondArgTag == LogicalExpressionTag.VARIABLE || secondArgTag == LogicalExpressionTag.FUNCTION_CALL) {
                argExpr = funcExpr.getArguments().get(1).getValue();
            }
        }

        if (argExpr != null) { // only one argument of the LogicalOp can be a variable (for [min:max] range computation)
            List<Mutable<ILogicalExpression>> aggFnArgs = new ArrayList<>(1);
            aggFnArgs.add(new MutableObject<>(argExpr));
            List<LogicalVariable> aggVarList = new ArrayList<>(1);
            aggVarList.add(optCtx.newVar());
            aggVarList.add(optCtx.newVar());

            List<Mutable<ILogicalExpression>> aggExprList = new ArrayList<>(1);
            BuiltinFunctionInfo minFn = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SQL_MIN);
            AggregateFunctionCallExpression minExpr = new AggregateFunctionCallExpression(minFn, false, aggFnArgs);
            aggExprList.add(new MutableObject<>(minExpr));
            BuiltinFunctionInfo maxFn = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SQL_MAX);
            AggregateFunctionCallExpression maxExpr = new AggregateFunctionCallExpression(maxFn, false, aggFnArgs);
            aggExprList.add(new MutableObject<>(maxExpr));

            AggregateOperator newAggOp = new AggregateOperator(aggVarList, aggExprList);
            if (needFullRange) { // without any predicates, find [min:max] range, i.e. full range
                ILogicalOperator scanOp = findAssignOrDataScanOp(selOp);
                ILogicalOperator copyOfScanOp = OperatorManipulationUtil.bottomUpCopyOperators(scanOp);
                newAggOp.getInputs().add(new MutableObject<>(copyOfScanOp));
            } else {
                ILogicalOperator copyOfSelOp = OperatorManipulationUtil.bottomUpCopyOperators(selOp);
                newAggOp.getInputs().add(new MutableObject<>(copyOfSelOp));
            }

            Mutable<ILogicalOperator> newAggOpRef = new MutableObject<>(newAggOp);
            OperatorPropertiesUtil.typeOpRec(newAggOpRef, optCtx);
            List<List<IAObject>> result =
                    AnalysisUtil.runQuery(newAggOpRef, aggVarList, optCtx, IRuleSetFactory.RuleSetKind.SAMPLING);

            // NOTE: consider scaling only for predicates of numeric attributes
            if (result.get(0).size() == 2) {
                IAObject minValueObj = result.get(0).get(0), maxValueObj = result.get(0).get(1);
                IAType resultType = minValueObj.getType();
                if (resultType == BuiltinType.AINT32 || resultType == BuiltinType.AINT64
                        || resultType == BuiltinType.AFLOAT || resultType == BuiltinType.ADOUBLE) {
                    minMaxValues[0] = getDouble(minValueObj);
                    minMaxValues[1] = getDouble(maxValueObj);
                }
            }
        }
        return minMaxValues;
    }

    private double getDouble(IAObject object) {
        IAType type = object.getType();
        if (type == BuiltinType.AINT32) {
            return ((AInt32) object).getIntegerValue();
        } else if (type == BuiltinType.AINT64) {
            return ((AInt64) object).getLongValue();
        } else if (type == BuiltinType.AFLOAT) {
            return ((AFloat) object).getFloatValue();
        } else if (type == BuiltinType.ADOUBLE) {
            return ((ADouble) object).getDoubleValue();
        }
        return Double.MAX_VALUE;
    }

    private double distinctEstimator(double estCardinality, double origDatasetCardinality) {
        if (totalSamples <= 1) {
            totalSamples += 2;
            estCardinality = totalSamples - 1;
        } else if (estCardinality == totalSamples) {
            estCardinality--;
        }
        setDistinctFromSamples(estCardinality);

        if (distinctEstimatorType.equals(DistinctEstimatorType.MMO)) {
            double denominator = derivativeFunctionForMMO(estCardinality);
            if (denominator == 0.0) { // Newton-Raphson method requires it to be non-zero
                return estCardinality;
            }
            double fraction = functionForMMO(estCardinality) / denominator;
            while (Math.abs(fraction) >= 0.001) {
                denominator = derivativeFunctionForMMO(estCardinality);
                if (denominator == 0.0) {
                    break;
                }
                fraction = functionForMMO(estCardinality) / denominator;
                estCardinality = estCardinality - fraction;
                if (estCardinality > origDatasetCardinality) {
                    estCardinality = origDatasetCardinality; // for preventing infinite growth beyond N
                    break;
                }
            }
        } else if (distinctEstimatorType.equals(DistinctEstimatorType.GEE)) {
            // TODO: complete GEE estimator for an operator, or delete it
            double uniqueCard = 1.0; // unique distinct values cardinality. FIX IT!!
            estCardinality = distinctFromSamples + Math.sqrt(origDatasetCardinality / totalSamples) * uniqueCard;
        } else {
            estCardinality = 0.0; // for neither of MMO or GEE estimators, cardinality should be 0
        }
        return estCardinality;
    }

    private double functionForMMO(double x) {
        //return (distinctFromSamples / x - 1.0) + Math.exp(-1.0 * numberSamples / x);
        return (x * (1.0 - Math.exp(-1.0 * totalSamples / x)) - distinctFromSamples);
        //double part = (1.0 - 1.0 / x);
        //return x * Math.pow(part, totalSamples) - x + distinctFromSamples;
    }

    private double derivativeFunctionForMMO(double x) {
        double arg = (totalSamples / x);
        return (1.0 - (arg + 1.0) * Math.exp(-1.0 * arg));
        //double part = (1.0 - 1.0 / x);
        //return Math.pow(part, totalSamples) - totalSamples / x * Math.pow(part, totalSamples - 1) - 1;
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

    private SelectOperator createSelectOp(SelectOperator selOp, Mutable<ILogicalExpression> argExpr)
            throws AlgebricksException {
        ILogicalOperator nextOp = selOp.getInputs().get(0).getValue();
        LogicalOperatorTag tag = nextOp.getOperatorTag();
        if (tag == LogicalOperatorTag.ASSIGN || tag == LogicalOperatorTag.DATASOURCESCAN) {
            ILogicalOperator copyOfNextOp = OperatorManipulationUtil.bottomUpCopyOperators(nextOp);
            SelectOperator newSelOp = new SelectOperator(argExpr);
            newSelOp.getInputs().add(new MutableObject<>(copyOfNextOp));
            return newSelOp;
        }
        return null;
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

    private ILogicalOperator findAssignOrDataScanOp(ILogicalOperator op) {
        LogicalOperatorTag tag = op.getOperatorTag();
        while (tag != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            tag = op.getOperatorTag();
            if (tag == LogicalOperatorTag.ASSIGN || tag == LogicalOperatorTag.DATASOURCESCAN) {
                return op;
            }
            op = op.getInputs().get(0).getValue();
        }
        return null;
    }
}
