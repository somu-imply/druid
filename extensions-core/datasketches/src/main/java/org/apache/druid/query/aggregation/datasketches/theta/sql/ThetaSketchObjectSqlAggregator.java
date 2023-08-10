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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

import java.util.Collections;

public class ThetaSketchObjectSqlAggregator extends ThetaSketchBaseSqlAggregator implements SqlAggregator
{
  private static final String NAME = "DS_THETA";
  private static final SqlAggFunction FUNCTION_INSTANCE =
      OperatorConversions.aggregatorBuilder(NAME)
                         .operandNames("column", "size")
                         .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
                         .operandTypeInference(InferTypes.VARCHAR_1024)
                         .requiredOperands(1)
                         .literalOperands(1)
                         .returnTypeInference(ThetaSketchSqlOperators.RETURN_TYPE_INFERENCE)
                         .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
                         .build();

  public ThetaSketchObjectSqlAggregator()
  {
    super(false);
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  protected Aggregation toAggregation(
      String name,
      boolean finalizeAggregations,
      AggregatorFactory aggregatorFactory
  )
  {
    return Aggregation.create(
        Collections.singletonList(aggregatorFactory),
        null
    );
  }
}
