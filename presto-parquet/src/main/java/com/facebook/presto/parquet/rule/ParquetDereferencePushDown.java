/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public abstract class ParquetDereferencePushDown
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;

    public ParquetDereferencePushDown(RowExpressionService rowExpressionService)
    {
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return ConnectorPlanRewriter.rewriteWith(new Rewriter(session, variableAllocator, idAllocator), maxSubplan);
    }

    /**
     * Whether Parquet dereference pushdown is enabled for given TableHandle
     */
    protected abstract boolean isParquetDereferenceEnabled(ConnectorSession session, TableHandle tableHandle);

    /**
     * ColumnHandle is an interface. Each connector implements its own version.
     * Connector specific implementation of this method returns the column name given ColumnHandle refers to.
     */
    protected abstract String getColumnName(ColumnHandle columnHandle);

    /**
     * Create connector specific ColumnHandle for given subfield that is being pushed into table scan.
     *
     * @param baseColumnHandle ColumnHandle for base column that given <i>subfield</i> is part of.
     *                            Ex. in "msg.a.b", "msg" is the top level column. "a.b" is the subfield
     *                            that is part of "msg". This ColumnHandle refers to "msg".
     * @param subfield            Subfield info.
     * @param subfieldDataType    Data type of the subfield.
     * @param subfieldColumnName  Name of the subfield column being referred in table scan output.
     * @return
     */
    protected abstract ColumnHandle createSubfieldColumnHandle(
            ColumnHandle baseColumnHandle,
            Subfield subfield,
            Type subfieldDataType,
            String subfieldColumnName);

    private Map<RowExpression, Subfield> extractDereferences(
            Map<String, ColumnHandle> baseColumnHandles,
            ConnectorSession session, ExpressionOptimizer expressionOptimizer,
            Set<RowExpression> expressions)
    {
        Set<RowExpression> dereferenceAndVariableExpressions = new HashSet<>();
        expressions.forEach(e -> e.accept(new ExtractDereferenceAndVariables(session, expressionOptimizer), dereferenceAndVariableExpressions));

        // keep prefix only expressions
        List<RowExpression> dereferences = dereferenceAndVariableExpressions.stream()
                .filter(expression -> !prefixExists(expression, dereferenceAndVariableExpressions))
                .filter(expression -> expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE)
                .collect(Collectors.toList());

        return dereferences.stream().collect(toMap(identity(), dereference -> createNestedColumn(
                baseColumnHandles, dereference, expressionOptimizer, session)));
    }

    private static boolean prefixExists(RowExpression expression, Set<RowExpression> allExpressions)
    {
        int[] referenceCount = {0};
        expression.accept(
                new DefaultRowExpressionTraversalVisitor<int[]>()
                {
                    @Override
                    public Void visitSpecialForm(SpecialFormExpression specialForm, int[] context)
                    {
                        if (specialForm.getForm() != DEREFERENCE) {
                            return super.visitSpecialForm(specialForm, context);
                        }

                        if (allExpressions.contains(specialForm)) {
                            referenceCount[0] += 1;
                        }

                        RowExpression base = specialForm.getArguments().get(0);
                        base.accept(this, context);
                        return null;
                    }

                    @Override
                    public Void visitVariableReference(VariableReferenceExpression reference, int[] context)
                    {
                        if (allExpressions.contains(reference)) {
                            referenceCount[0] += 1;
                        }
                        return null;
                    }
                }, referenceCount);

        return referenceCount[0] > 1;
    }

    private Subfield createNestedColumn(
            Map<String, ColumnHandle> baseColumnHandles,
            RowExpression rowExpression,
            ExpressionOptimizer expressionOptimizer,
            ConnectorSession session)
    {
        if (!(rowExpression instanceof SpecialFormExpression) || ((SpecialFormExpression) rowExpression).getForm() != DEREFERENCE) {
            throw new IllegalArgumentException("expecting SpecialFormExpression(DEREFERENCE), but got: " + rowExpression);
        }

        RowExpression currentRowExpression = rowExpression;
        List<Subfield.PathElement> elements = new ArrayList<>();
        while (true) {
            if (currentRowExpression instanceof VariableReferenceExpression) {
                Collections.reverse(elements);
                String name = ((VariableReferenceExpression) currentRowExpression).getName();
                ColumnHandle handle = baseColumnHandles.get(name);
                checkArgument(handle != null, "Missing Column handle: " + name);
                String originalColumnName = getColumnName(handle);
                return new Subfield(originalColumnName, unmodifiableList(elements));
            }

            if (currentRowExpression instanceof SpecialFormExpression && ((SpecialFormExpression) currentRowExpression).getForm() == DEREFERENCE) {
                SpecialFormExpression dereferenceExpression = (SpecialFormExpression) currentRowExpression;
                RowExpression base = dereferenceExpression.getArguments().get(0);
                RowType baseType = (RowType) base.getType();

                RowExpression indexExpression = expressionOptimizer.optimize(
                        dereferenceExpression.getArguments().get(1),
                        ExpressionOptimizer.Level.OPTIMIZED,
                        session);

                if (indexExpression instanceof ConstantExpression) {
                    Object index = ((ConstantExpression) indexExpression).getValue();
                    if (index instanceof Number) {
                        Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                        if (fieldName.isPresent()) {
                            elements.add(new Subfield.NestedField(fieldName.get()));
                            currentRowExpression = base;
                            continue;
                        }
                    }
                }
            }
            break;
        }

        throw new IllegalArgumentException("expecting SpecialFormExpression(DEREFERENCE) with constants for indices, but got: " + currentRowExpression);
    }

    /**
     * Visitor to extract all dereference expressions and variable references.
     * <p>
     * If a dereference expression contains dereference expression, inner dereference expression are not returned
     * * sub(deref(deref(x, 1), 2)) --> deref(deref(x,1), 2)
     * Variable expressions returned are the ones not referenced by the dereference expressions
     * * sub(x + 1) --> x
     * * sub(deref(x, 1)) -> deref(x,1)
     */
    private static class ExtractDereferenceAndVariables
            extends DefaultRowExpressionTraversalVisitor<Set<RowExpression>>
    {
        private final ConnectorSession connectorSession;
        private final ExpressionOptimizer expressionOptimizer;

        public ExtractDereferenceAndVariables(ConnectorSession connectorSession, ExpressionOptimizer expressionOptimizer)
        {
            this.connectorSession = connectorSession;
            this.expressionOptimizer = expressionOptimizer;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, Set<RowExpression> context)
        {
            if (specialForm.getForm() != DEREFERENCE) {
                return super.visitSpecialForm(specialForm, context);
            }

            RowExpression expression = specialForm;
            while (true) {
                if (expression instanceof VariableReferenceExpression) {
                    context.add(specialForm);
                    return null;
                }

                if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE) {
                    SpecialFormExpression dereferenceExpression = (SpecialFormExpression) expression;
                    RowExpression base = dereferenceExpression.getArguments().get(0);
                    RowType baseType = (RowType) base.getType();

                    RowExpression indexExpression = expressionOptimizer.optimize(
                            dereferenceExpression.getArguments().get(1),
                            ExpressionOptimizer.Level.OPTIMIZED,
                            connectorSession);

                    if (indexExpression instanceof ConstantExpression) {
                        Object index = ((ConstantExpression) indexExpression).getValue();
                        if (index instanceof Number) {
                            Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                            if (fieldName.isPresent()) {
                                expression = base;
                                continue;
                            }
                        }
                    }
                }
                break;
            }

            return super.visitSpecialForm(specialForm, context);
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Set<RowExpression> context)
        {
            context.add(reference);
            return null;
        }
    }

    private static class DereferenceExpressionRewriter
            extends RowExpressionRewriter<Void>
    {
        private final Map<RowExpression, VariableReferenceExpression> dereferenceMap;

        public DereferenceExpressionRewriter(Map<RowExpression, VariableReferenceExpression> dereferenceMap)
        {
            this.dereferenceMap = dereferenceMap;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            return dereferenceMap.get(node);
        }
    }

    /**
     * Looks for ProjectNode -> TableScanNode patterns. Goes through the project expressions to extract out the DEREFERENCE expressions,
     * pushes the dereferences down to TableScan and creates new project expressions with the pushed down column coming from the TableScan.
     * Returned plan nodes could contain unreferenced outputs which will be pruned later in the planning process.
     */
    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;

        Rewriter(ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode project, RewriteContext<Void> context)
        {
            if (!(project.getSource() instanceof TableScanNode)) {
                return visitPlan(project, context);
            }

            TableScanNode tableScan = (TableScanNode) project.getSource();
            if (!isParquetDereferenceEnabled(session, tableScan.getTable())) {
                return visitPlan(project, context);
            }
            Map<String, ColumnHandle> baseColumnHandles = new HashMap<>();
            tableScan.getAssignments().entrySet().forEach(assignment -> {
                baseColumnHandles.put(assignment.getKey().getName(), assignment.getValue());
                baseColumnHandles.put(getColumnName(assignment.getValue()), assignment.getValue());
            });

            Map<RowExpression, Subfield> dereferenceToNestedColumnMap = extractDereferences(
                    baseColumnHandles,
                    session,
                    rowExpressionService.getExpressionOptimizer(session),
                    new HashSet<>(project.getAssignments().getExpressions()));
            if (dereferenceToNestedColumnMap.isEmpty()) {
                return visitPlan(project, context);
            }

            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>(tableScan.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>(tableScan.getAssignments());

            Map<RowExpression, VariableReferenceExpression> dereferenceToVariableMap = new HashMap<>();

            for (Map.Entry<RowExpression, Subfield> dereference : dereferenceToNestedColumnMap.entrySet()) {
                Subfield subfield = dereference.getValue();
                RowExpression dereferenceExpression = dereference.getKey();

                // Find the nested column Hive Type
                ColumnHandle baseColumnHandle = baseColumnHandles.get(subfield.getRootName());
                if (baseColumnHandle == null) {
                    throw new IllegalArgumentException("Subfield column [" + subfield + "]'s base column " + subfield.getRootName() + " is not present in table scan output");
                }
                String subfieldColumnName = pushdownColumnNameForSubfield(subfield);

                ColumnHandle nestedColumnHandle = createSubfieldColumnHandle(
                        baseColumnHandle,
                        subfield,
                        dereferenceExpression.getType(),
                        subfieldColumnName);

                VariableReferenceExpression newOutputVariable = variableAllocator.newVariable(subfieldColumnName, dereferenceExpression.getType());
                newOutputVariables.add(newOutputVariable);
                newAssignments.put(newOutputVariable, nestedColumnHandle);

                dereferenceToVariableMap.put(dereferenceExpression, newOutputVariable);
            }

            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScan.getTable(),
                    newOutputVariables,
                    newAssignments,
                    tableScan.getTableConstraints(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());

            Assignments.Builder newProjectAssignmentBuilder = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
                RowExpression newExpression = RowExpressionTreeRewriter.rewriteWith(new DereferenceExpressionRewriter(dereferenceToVariableMap), entry.getValue());
                newProjectAssignmentBuilder.put(entry.getKey(), newExpression);
            }

            return new ProjectNode(tableScan.getSourceLocation(), idAllocator.getNextId(), newTableScan, newProjectAssignmentBuilder.build(), project.getLocality());
        }
    }
}
