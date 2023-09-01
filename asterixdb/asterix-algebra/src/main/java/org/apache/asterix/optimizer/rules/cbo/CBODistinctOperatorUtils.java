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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CBODistinctOperatorUtils {

    public static boolean containsAllGroupByDistinctVarsInScanOp(DataSourceScanOperator scanOp,
            ILogicalOperator grpByDistinctOp) {
        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();
        if (tag == LogicalOperatorTag.GROUP || tag == LogicalOperatorTag.DISTINCT) {
            List<LogicalVariable> distinctVars = getGroupByDistinctVarFuncPair(grpByDistinctOp).first;
            if (distinctVars.size() == 0) {
                return false;
            }
            List<LogicalVariable> scanVars = scanOp.getVariables();
            List<LogicalVariable> foundDistinctVars = new ArrayList<>();
            for (LogicalVariable scanVar : scanVars) {
                if (distinctVars.contains(scanVar)) {
                    foundDistinctVars.add(scanVar);
                }
            }
            // discarding the variable for Dataset name or alias from scanOp
            return ((scanVars.size() - 1) == foundDistinctVars.size());
        }
        return false;
    }

    public static void createDistinctOpsForJoinNodes(ILogicalOperator op, ILogicalOperator grpByDistinctOp,
            IOptimizationContext context, HashMap<DataSourceScanOperator, ILogicalOperator> map) {
        List<LogicalVariable> foundDistinctVars = new ArrayList<>();
        ILogicalOperator selOp = null, assignOp = null;

        LogicalOperatorTag tag = op.getOperatorTag();
        // add DistinctOp to count distinct values in an attribute (except all PK attribute(s))
        if (tag == LogicalOperatorTag.ASSIGN || tag == LogicalOperatorTag.SELECT
                || tag == LogicalOperatorTag.DATASOURCESCAN) {
            Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> distinctPair =
                    getGroupByDistinctVarFuncPair(grpByDistinctOp);
            List<LogicalVariable> distinctVars = distinctPair.first;
            if (distinctVars.size() == 0) {
                return;
            }

            DataSourceScanOperator scanOp = null;
            LogicalVariable assignVar;
            while (tag != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                if (tag == LogicalOperatorTag.SELECT) {
                    selOp = op;
                } else if (tag == LogicalOperatorTag.ASSIGN) {
                    assignVar = ((AssignOperator) op).getVariables().get(0);
                    int idx = distinctVars.indexOf(assignVar);
                    if (idx != -1 && assignOp == null) { // first corresponding AssignOp found
                        assignOp = op;
                    }
                    if (idx != -1) { // add all variables of the AssignOp
                        foundDistinctVars.add(assignVar);
                    }
                } else if (tag == LogicalOperatorTag.DATASOURCESCAN) {
                    scanOp = (DataSourceScanOperator) op;
                    // will work for any attributes present in GroupByOp or DistinctOp
                    // Note: uncomment the following if-else-statement if CBO doesn't need to estimate the number of distinct values
                    // when GroupByOp or DistinctOp contains all PK attributes
                    // (in the case of estimated cardinality from samples can be mostly same as original dataset cardinality)
                    /*if (containsAllGroupByDistinctVarsInScanOp(scanOp, grpByDistinctOp)) {
                        // contains all PK attribute(s), so estimated distinct cardinality is same as original dataset cardinality
                        scanOp = null;
                    } else { // at least one PK attribute is not in GroupByOp or DistinctOp variables */
                    List<LogicalVariable> scanVars = scanOp.getVariables();
                    for (LogicalVariable scanVar : scanVars) { // add all required variables of the DataSourceScanOp
                        if (distinctVars.contains(scanVar)) {
                            foundDistinctVars.add(scanVar);
                        }
                    }
                    if (foundDistinctVars.size() == 0) {
                        scanOp = null; // GroupByOp or DistinctOp doesn't contain any attributes of the dataset
                    }
                    //}
                }
                op = op.getInputs().get(0).getValue();
                tag = op.getOperatorTag();
            }

            if (scanOp != null) {
                ILogicalOperator inputOp = (selOp != null) ? selOp : ((assignOp != null) ? assignOp : scanOp);
                SourceLocation sourceLocation = inputOp.getSourceLocation();
                DistinctOperator distinctOp =
                        createDistinctOp(foundDistinctVars, inputOp, sourceLocation, distinctPair.second, context);
                if (distinctOp != null) {
                    map.put(scanOp, distinctOp);
                }
            }
        } else if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
            for (int i = 0; i < op.getInputs().size(); i++) {
                ILogicalOperator nextOp = op.getInputs().get(i).getValue();
                createDistinctOpsForJoinNodes(nextOp, grpByDistinctOp, context, map);
            }
        }
    }

    private static List<LogicalVariable> getFunctionVariables(AbstractFunctionCallExpression funcExpr) {
        List<LogicalVariable> variables = new ArrayList<>();
        List<Mutable<ILogicalExpression>> argList = funcExpr.getArguments();
        for (Mutable<ILogicalExpression> arg : argList) {
            if (arg.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                variables.add(((VariableReferenceExpression) arg.getValue()).getVariableReference());
            } else if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                variables.addAll(getFunctionVariables((AbstractFunctionCallExpression) arg.getValue()));
            }
        }
        return variables;
    }

    private static Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> getGroupByDistinctVarFuncPair(
            ILogicalOperator grpByDistinctOp) {
        List<LogicalVariable> distinctVars = new ArrayList<>();
        List<AbstractFunctionCallExpression> distinctFunctions = new ArrayList<>();
        ILogicalExpression varRef;
        ILogicalOperator nextOp;
        if (grpByDistinctOp.getOperatorTag() == LogicalOperatorTag.DISTINCT) {
            nextOp = grpByDistinctOp.getInputs().get(0).getValue();
            if (nextOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                ILogicalExpression assignExpr = ((AssignOperator) nextOp).getExpressions().get(0).getValue();
                if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) { // FId: open-object-constructor
                    List<Mutable<ILogicalExpression>> argList =
                            ((AbstractFunctionCallExpression) assignExpr).getArguments();
                    for (int i = 0; i < argList.size(); i += 2) {
                        varRef = argList.get(i + 1).getValue();
                        if (varRef.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                            distinctVars.add(((VariableReferenceExpression) varRef).getVariableReference());
                        } else if (varRef.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            distinctVars.addAll(getFunctionVariables((AbstractFunctionCallExpression) varRef));
                            distinctFunctions.add((AbstractFunctionCallExpression) varRef);
                        }
                    }
                }
            }
        } else if (grpByDistinctOp.getOperatorTag() == LogicalOperatorTag.GROUP) {
            distinctVars = ((GroupByOperator) grpByDistinctOp).getGroupByVarList();
            nextOp = grpByDistinctOp.getInputs().get(0).getValue();
            LogicalOperatorTag tag = nextOp.getOperatorTag();
            while (tag != LogicalOperatorTag.DATASOURCESCAN) {
                if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
                    break;
                } else if (tag == LogicalOperatorTag.ASSIGN) {
                    ILogicalExpression assignExpr = ((AssignOperator) nextOp).getExpressions().get(0).getValue();
                    if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        List<LogicalVariable> fVars = getFunctionVariables((AbstractFunctionCallExpression) assignExpr);
                        LogicalVariable assignVar = ((AssignOperator) nextOp).getVariables().get(0);
                        int idx = distinctVars.indexOf(assignVar);
                        if (idx != -1 && fVars.size() > 0) {
                            distinctVars.remove(idx);
                            distinctVars.addAll(fVars);
                            distinctFunctions.add((AbstractFunctionCallExpression) assignExpr);
                        }
                    }
                }
                nextOp = nextOp.getInputs().get(0).getValue();
                tag = nextOp.getOperatorTag();
            }
        }
        return new Pair<>(distinctVars, distinctFunctions);
    }

    private static AssignOperator createAssignOpForFunctionExpr(IOptimizationContext optCtx,
            List<LogicalVariable> distinctVars, List<AbstractFunctionCallExpression> funcExpr,
            SourceLocation sourceLocation) {
        int counter = 1;
        List<LogicalVariable> notFoundDistinctVars = new ArrayList<>(distinctVars);
        List<Mutable<ILogicalExpression>> openRecConsArgs = new ArrayList<>();
        for (AbstractFunctionCallExpression expr : funcExpr) {
            List<LogicalVariable> funcVars = getFunctionVariables(expr);
            if (new HashSet<>(distinctVars).containsAll(funcVars)) { // all variables in the function are of the current dataset
                openRecConsArgs.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(String.valueOf(counter))))));
                openRecConsArgs.add(new MutableObject<>(expr));
                counter++;
                notFoundDistinctVars.removeAll(funcVars); // DistinctOp variables are found in the function, so remove
            }
        }
        if (openRecConsArgs.size() > 0) { // at least one Function expression is available/applicable
            for (LogicalVariable var : notFoundDistinctVars) {
                openRecConsArgs.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(String.valueOf(counter))))));
                openRecConsArgs.add(new MutableObject<>(new VariableReferenceExpression(var)));
                counter++;
            }
            AbstractFunctionCallExpression openRecFunc = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), openRecConsArgs);
            LogicalVariable assignVar = optCtx.newVar();
            AssignOperator assignOp = new AssignOperator(assignVar, new MutableObject<>(openRecFunc));
            assignOp.setSourceLocation(sourceLocation);
            return assignOp;
        }
        return null;
    }

    private static DistinctOperator createDistinctOp(List<LogicalVariable> distinctVars, ILogicalOperator inputOp,
            SourceLocation sourceLocation, List<AbstractFunctionCallExpression> funcExpr, IOptimizationContext optCtx) {
        if (distinctVars.size() == 0 || inputOp == null) {
            return null;
        }
        LogicalOperatorTag tag = inputOp.getOperatorTag();
        if (tag != LogicalOperatorTag.ASSIGN && tag != LogicalOperatorTag.SELECT
                && tag != LogicalOperatorTag.DATASOURCESCAN) {
            return null;
        }

        // create an AssignOp for Function expressions of the corresponding GroupByOp or DistinctOp
        AssignOperator assignOp = createAssignOpForFunctionExpr(optCtx, distinctVars, funcExpr, sourceLocation);

        List<Mutable<ILogicalExpression>> distinctExpr = new ArrayList<>();
        if (assignOp == null) { // no Function expressions are available/applicable for the new DistinctOp
            for (LogicalVariable var : distinctVars) {
                VariableReferenceExpression varExpr = new VariableReferenceExpression(var);
                varExpr.setSourceLocation(sourceLocation);
                Mutable<ILogicalExpression> vRef = new MutableObject<>(varExpr);
                distinctExpr.add(vRef);
            }
        } else {
            VariableReferenceExpression varExpr = new VariableReferenceExpression(assignOp.getVariables().get(0));
            varExpr.setSourceLocation(sourceLocation);
            distinctExpr.add(new MutableObject<>(varExpr));
        }

        // create a new Distinct operator
        DistinctOperator distinctOp = new DistinctOperator(distinctExpr);
        distinctOp.setSourceLocation(sourceLocation);
        if (assignOp == null) {
            distinctOp.getInputs().add(new MutableObject<>(inputOp));
        } else {
            distinctOp.getInputs().add(new MutableObject<>(assignOp));
            ILogicalOperator nextOp = distinctOp.getInputs().get(0).getValue();
            nextOp.getInputs().add(new MutableObject<>(inputOp));
        }
        distinctOp.setExecutionMode(inputOp.getExecutionMode());

        return distinctOp;
    }
}
