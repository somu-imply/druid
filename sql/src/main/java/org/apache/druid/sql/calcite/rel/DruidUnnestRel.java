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

package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * DruidRel that uses a {@link org.apache.druid.query.UnnestDataSource}.
 */
public class DruidUnnestRel extends DruidRel<DruidUnnestRel>
{

  private final PartialDruidQuery partialQuery;
  private final Uncollect uncollectRel;
  private final PlannerConfig plannerConfig;
  private LogicalProject logicalProject;
  private LogicalValues logicalValues;
  private final DataSource baseDataSource;

  public DruidUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Uncollect uncollectRel,
      LogicalProject logicalProject,
      LogicalValues logicalValues,
      PartialDruidQuery partialQuery,
      DataSource baseDataSource,
      PlannerContext plannerContext
  ) {
    super(cluster, traitSet, plannerContext);
    this.uncollectRel = uncollectRel;
    this.partialQuery = partialQuery;
    this.logicalProject = logicalProject;
    this.logicalValues = logicalValues;
    this.plannerConfig = plannerContext.getPlannerConfig();
    this.baseDataSource = baseDataSource;
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidUnnestRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        uncollectRel,
        logicalProject,
        logicalValues,
        newQueryBuilder,
        baseDataSource,
        getPlannerContext()
        );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    RowSignature rs = RowSignatures.fromRelDataType(logicalProject.getRowType().getFieldNames(),logicalProject.getRowType());
    RexCall rx = (RexCall) logicalProject.getChildExps().get(0);
    RexFieldAccess rf = (RexFieldAccess)rx.getOperands().get(0);
    String dimensionToUnnest = rf.getField().getName();

    
    /*
    When this is called only from Uncollect, this should create an InlineDataSource
    and pass it as the base of an UnnestDataSource with a dummy input




    InlineDataSource id = InlineDataSource.fromIterable(
        logicalProject.getChildExps(),
        RowSignatures.fromRelDataType(
          logicalProject.getRowType().getFieldNames(),
          logicalProject.getRowType()
    ));

    UnnestDataSource unnestDataSource = UnnestDataSource.create(id, "dummy", "dummy", null);

    return partialQuery.build(
        unnestDataSource,
        RowSignatures.fromRelDataType(
            logicalProject.getRowType().getFieldNames(),
            logicalProject.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );*/
    return null;
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return null;
  }

  @Override
  public DruidUnnestRel asDruidConvention()
  {
    return null;
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return null;
  }
}
