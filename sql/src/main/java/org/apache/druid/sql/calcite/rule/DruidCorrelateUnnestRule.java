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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidCorrelateUnnestRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnnestDatasourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.ArrayList;
import java.util.List;

public class DruidCorrelateUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;
  private final boolean enableLeftScanDirect;

  public DruidCorrelateUnnestRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            LogicalCorrelate.class,
            operand(DruidRel.class, any()),
            operand(DruidUnnestDatasourceRel.class, any())
        )
    );

    this.plannerContext = plannerContext;
    this.enableLeftScanDirect = plannerContext.queryContext().getEnableJoinLeftScanDirect();
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate logicalCorrelate = call.rel(0);
    final DruidRel<?> druidQueryRel = call.rel(1);
    DruidUnnestDatasourceRel unnestDatasourceRel = call.rel(2);
    final Filter leftFilter;
    final DruidRel<?> newLeft;
    final RexBuilder rexBuilder = logicalCorrelate.getCluster().getRexBuilder();
    final List<RexNode> newProjectExprs = new ArrayList<>();

    final boolean isLeftDirectAccessPossible = enableLeftScanDirect && (druidQueryRel instanceof DruidQueryRel);

    if (druidQueryRel.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT
        && (isLeftDirectAccessPossible || druidQueryRel.getPartialDruidQuery().getWhereFilter() == null)) {
      // Swap the left-side projection above the correlate, so the left side is a simple scan or mapping. This helps us
      // avoid subqueries.
      final RelNode leftScan = druidQueryRel.getPartialDruidQuery().getScan();
      final Project leftProject = druidQueryRel.getPartialDruidQuery().getSelectProject();
      leftFilter = druidQueryRel.getPartialDruidQuery().getWhereFilter();

      // Left-side projection expressions rewritten to be on top of the correlate.
      newProjectExprs.addAll(leftProject.getProjects());
      newLeft = druidQueryRel.withPartialQuery(PartialDruidQuery.create(leftScan));
    } else {
      // Leave left as-is. Write input refs that do nothing.
      for (int i = 0; i < druidQueryRel.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(rexBuilder.makeInputRef(logicalCorrelate.getRowType().getFieldList().get(i).getType(), i));
      }
      newLeft = druidQueryRel;
      leftFilter = null;
    }


    if (unnestDatasourceRel.getPartialDruidQuery().stage() == PartialDruidQuery.Stage.SELECT_PROJECT) {
      for (final RexNode rexNode : RexUtil.shift(
          unnestDatasourceRel.getPartialDruidQuery()
                             .getSelectProject()
                             .getProjects(),
          newLeft.getRowType().getFieldCount()
      )) {
        newProjectExprs.add(rexNode);
      }
    } else {
      // Leave right as-is. Write input refs that do nothing.
      for (int i = 0; i < unnestDatasourceRel.getRowType().getFieldCount(); i++) {
        newProjectExprs.add(
            rexBuilder.makeInputRef(
                logicalCorrelate.getRowType()
                                .getFieldList()
                                .get(druidQueryRel.getRowType().getFieldCount() + i)
                                .getType(),
                newLeft.getRowType().getFieldCount() + i
            )
        );
      }
    }


    // todo: make new projects with druidQueryRel projects + unnestRel projects shifted
    LogicalCorrelate lc = logicalCorrelate.copy(
        logicalCorrelate.getTraitSet(),
        newLeft,
        unnestDatasourceRel,
        logicalCorrelate.getCorrelationId(),
        logicalCorrelate.getRequiredColumns(),
        logicalCorrelate.getJoinType()
    );


    DruidCorrelateUnnestRel newRel = new DruidCorrelateUnnestRel(
        lc.getCluster(),
        lc.getTraitSet(),
        lc,
        PartialDruidQuery.create(lc),
        (DruidQueryRel) druidQueryRel,
        unnestDatasourceRel,
        leftFilter,
        plannerContext
    );


    final RelBuilder relBuilder =
        call.builder()
            .push(newRel)
            .project(RexUtil.fixUp(
                rexBuilder,
                newProjectExprs,
                RelOptUtil.getFieldTypeList(lc.getRowType())
            ));

    call.transformTo(relBuilder.build());
  }
}
