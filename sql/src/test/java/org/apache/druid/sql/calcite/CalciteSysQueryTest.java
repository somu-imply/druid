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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.sql.calcite.DecoupledIgnore.DecoupledIgnoreProcessor;
import org.apache.druid.sql.calcite.DecoupledIgnore.Modes;
import org.junit.Rule;
import org.junit.Test;

public class CalciteSysQueryTest extends BaseCalciteQueryTest
{
  @Rule(order = 0)
  public DecoupledIgnoreProcessor decoupledIgnoreProcessor = new DecoupledIgnoreProcessor();

  @Test
  public void testTasksSum()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}))
        .expectedLogicalPlan("LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
            + "  LogicalProject(exprs=[[$3, $8]])\n"
            + "    LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  @DecoupledIgnore(mode = Modes.EXPRESSION_NOT_GROUPED)
  @Test
  public void testTasksSumOver()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) over () from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}))
        // please add expectedLogicalPlan if this test starts passing!
        .run();
  }
}
