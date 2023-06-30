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

package org.apache.druid.sql.calcite.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Enclosed.class)
public class DruidSqlParserUtilsTest
{
  /**
   * Sanity checking that the formats of TIME_FLOOR(__time, Period) work as expected
   */
  @RunWith(Parameterized.class)
  public static class TimeFloorToGranularityConversionTest
  {
    @Parameterized.Parameters(name = "{1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{"PT1H", Granularities.HOUR}
      );
    }

    String periodString;
    Granularity expectedGranularity;

    public TimeFloorToGranularityConversionTest(String periodString, Granularity expectedGranularity)
    {
      this.periodString = periodString;
      this.expectedGranularity = expectedGranularity;
    }

    @Test
    public void testGranularityFromTimeFloor() throws ParseException
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString(this.periodString, SqlParserPos.ZERO));
      final SqlNode timeFloorCall = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(
          timeFloorCall);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }
  }

  /**
   * Sanity checking that FLOOR(__time TO TimeUnit()) works as intended with the supported granularities
   */
  @RunWith(Parameterized.class)
  public static class FloorToGranularityConversionTest
  {
    @Parameterized.Parameters(name = "{1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{TimeUnit.SECOND, Granularities.SECOND},
          new Object[]{TimeUnit.MINUTE, Granularities.MINUTE},
          new Object[]{TimeUnit.HOUR, Granularities.HOUR},
          new Object[]{TimeUnit.DAY, Granularities.DAY},
          new Object[]{TimeUnit.WEEK, Granularities.WEEK},
          new Object[]{TimeUnit.MONTH, Granularities.MONTH},
          new Object[]{TimeUnit.QUARTER, Granularities.QUARTER},
          new Object[]{TimeUnit.YEAR, Granularities.YEAR}
      );
    }

    TimeUnit timeUnit;
    Granularity expectedGranularity;

    public FloorToGranularityConversionTest(TimeUnit timeUnit, Granularity expectedGranularity)
    {
      this.timeUnit = timeUnit;
      this.expectedGranularity = expectedGranularity;
    }

    @Test
    public void testGetGranularityFromFloor() throws ParseException
    {
      // parserPos doesn't matter
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(this.timeUnit, null, SqlParserPos.ZERO));
      final SqlNode floorCall = SqlStdOperatorTable.FLOOR.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(floorCall);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }
  }

  /**
   * Test class that validates the resolution of "CLUSTERED BY" columns to output columns.
   */
  public static class ResolveClusteredByColumnsTest
  {
    @Test
    public void testNullClusteredByAndSource()
    {
      Assert.assertNull(DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(null, null));
    }

    @Test
    public void testNullClusteredBy()
    {
      final SqlNodeList selectArgs = new SqlNodeList(SqlParserPos.ZERO);
      selectArgs.add(new SqlIdentifier("__time", new SqlParserPos(0, 1)));
      Assert.assertNull(DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(
          null,
          new SqlSelect(SqlParserPos.ZERO, null, selectArgs, null, null, null, null, null, null, null, null, null, null)
        )
      );
    }

    @Test
    public void testNullSource()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));

      IllegalArgumentException iae = Assert.assertThrows(
          IllegalArgumentException.class,
          () -> DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(args, null)
      );
      Assert.assertEquals("Source node must be either SqlSelect or SqlOrderBy, but found [null]", iae.getMessage());
    }

    @Test
    public void testSimpleClusteredBy()
    {
      final SqlNodeList selectArgs = new SqlNodeList(SqlParserPos.ZERO);
      selectArgs.add(new SqlIdentifier("__time", new SqlParserPos(0, 1)));
      selectArgs.add(new SqlIdentifier("FOO", new SqlParserPos(0, 2)));
      selectArgs.add(new SqlIdentifier("BOO", new SqlParserPos(0, 3)));

      final SqlSelect sqlSelect = new SqlSelect(
          SqlParserPos.ZERO,
          null,
          selectArgs,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );

      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("FOO", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      Assert.assertEquals(
          Arrays.asList("__time", "FOO", "BOO"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, sqlSelect)
      );
    }

    @Test
    public void testClusteredByOrdinalInvalidThrowsException()
    {
      final SqlNodeList selectArgs = new SqlNodeList(SqlParserPos.ZERO);
      selectArgs.add(new SqlIdentifier("__time", new SqlParserPos(0, 1)));
      selectArgs.add(new SqlIdentifier("FOO", new SqlParserPos(0, 2)));
      selectArgs.add(new SqlIdentifier("BOO", new SqlParserPos(0, 3)));

      final SqlSelect sqlSelect = new SqlSelect(
          SqlParserPos.ZERO,
          null,
          selectArgs,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );

      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("FOO", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO));

      MatcherAssert.assertThat(
          Assert.assertThrows(DruidException.class, () -> DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, sqlSelect)),
          DruidExceptionMatcher.invalidSqlInput().expectMessageIs(
              "Ordinal[4] specified in the CLUSTERED BY clause is invalid. It must be between 1 and 3."
          )
      );
    }


    @Test
    public void testClusteredByOrdinalsAndAliases()
    {
      // Construct the select source args
      final SqlNodeList selectArgs = new SqlNodeList(SqlParserPos.ZERO);
      selectArgs.add(new SqlIdentifier("__time", new SqlParserPos(0, 1)));
      selectArgs.add(new SqlIdentifier("DIM3", new SqlParserPos(0, 2)));

      SqlBasicCall sqlBasicCall1 = new SqlBasicCall(
          new SqlAsOperator(),
          new SqlNode[]{
              new SqlIdentifier("DIM3", SqlParserPos.ZERO),
              new SqlIdentifier("DIM3_ALIAS", SqlParserPos.ZERO)
          },
          new SqlParserPos(0, 3)
      );
      selectArgs.add(sqlBasicCall1);

      SqlBasicCall sqlBasicCall2 = new SqlBasicCall(
          new SqlAsOperator(),
          new SqlNode[]{
              new SqlIdentifier("FLOOR(__time)", SqlParserPos.ZERO),
              new SqlIdentifier("floor_dim4_time", SqlParserPos.ZERO)
          },
          new SqlParserPos(0, 4)
      );
      selectArgs.add(sqlBasicCall2);

      selectArgs.add(new SqlIdentifier("DIM5", new SqlParserPos(0, 5)));
      selectArgs.add(new SqlIdentifier("DIM6", new SqlParserPos(0, 6)));

      final SqlNodeList args3 = new SqlNodeList(SqlParserPos.ZERO);
      args3.add(new SqlIdentifier("timestamps", SqlParserPos.ZERO));
      args3.add(SqlLiteral.createCharString("PT1H", SqlParserPos.ZERO));
      selectArgs.add(TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args3));

      final SqlSelect sqlSelect = new SqlSelect(
          SqlParserPos.ZERO,
          null,
          selectArgs,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );

      // Construct the clustered by args
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("7", SqlParserPos.ZERO));

      Assert.assertEquals(
          Arrays.asList("DIM3_ALIAS", "floor_dim4_time", "DIM5", "TIME_FLOOR(\"timestamps\", 'PT1H')"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, sqlSelect)
      );
    }

    @Test
    public void testSimpleClusteredByWithOrderBy()
    {
      final SqlNodeList selectArgs = new SqlNodeList(SqlParserPos.ZERO);
      selectArgs.add(new SqlIdentifier("__time", new SqlParserPos(0, 1)));
      selectArgs.add(new SqlIdentifier("FOO", new SqlParserPos(0, 2)));
      selectArgs.add(new SqlIdentifier("BOO", new SqlParserPos(0, 3)));

      final SqlSelect sqlSelect = new SqlSelect(
          SqlParserPos.ZERO,
          null,
          selectArgs,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );

      SqlNodeList orderList = new SqlNodeList(SqlParserPos.ZERO);
      orderList.add(sqlSelect);

      SqlNode orderByNode = new SqlOrderBy(SqlParserPos.ZERO, sqlSelect, orderList, null, null);

      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("FOO", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      Assert.assertEquals(
          Arrays.asList("__time", "FOO", "BOO"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, orderByNode)
      );
    }
  }

  public static class ClusteredByColumnsValidationTest
  {
    /**
     * Tests an empty CLUSTERED BY clause
     */
    @Test
    public void testEmptyClusteredByColumnsValid()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);

      DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs);
    }

    /**
     * Tests clause "CLUSTERED BY DIM1, DIM2 ASC, 3"
     */
    @Test
    public void testClusteredByColumnsValid()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("DIM1", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("DIM2 ASC", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs);
    }

    /**
     * Tests clause "CLUSTERED BY DIM1, DIM2 ASC, 3, DIM4 DESC"
     */
    @Test
    public void testClusteredByColumnsWithDescThrowsException()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("DIM1", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("DIM2 ASC", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      final SqlBasicCall sqlBasicCall = new SqlBasicCall(
          new SqlPostfixOperator("DESC", SqlKind.DESCENDING, 2, null, null, null),
          new SqlNode[]{
              new SqlIdentifier("DIM4", SqlParserPos.ZERO)
          },
          new SqlParserPos(0, 3)
      );
      clusteredByArgs.add(sqlBasicCall);

      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs("Invalid CLUSTERED BY clause [`DIM4` DESC]: cannot sort in descending order.")
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs));
    }
  }

  public static class FloorToGranularityConversionErrorsTest
  {
    /**
     * Tests clause like "PARTITIONED BY 'day'"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectNode()
    {
      SqlNode sqlNode = SqlLiteral.createCharString("day", SqlParserPos.ZERO);
      DruidException e = Assert.assertThrows(
          DruidException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      MatcherAssert.assertThat(
          e,
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageIs(
                  "Invalid granularity ['day'] after PARTITIONED BY.  "
                  + "Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR() or TIME_FLOOR()"
              )
      );
    }

    /**
     * Tests clause like "PARTITIONED BY CEIL(__time TO DAY)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectFunctionCall()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.CEIL.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals(
          "PARTITIONED BY clause only supports FLOOR(__time TO <unit> and TIME_FLOOR(__time, period) functions",
          e.getMessage()
      );
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(__time)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectNumberOfArguments()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("FLOOR in PARTITIONED BY clause must have two arguments", e.getMessage());
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(timestamps TO DAY)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithWrongIdentifierInFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("timestamps", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("First argument to FLOOR in PARTITIONED BY clause can only be __time", e.getMessage());
    }

    /**
     * Tests clause like "PARTITIONED BY TIME_FLOOR(timestamps, 'PT1H')"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithWrongIdentifierInTimeFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("timestamps", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString("PT1H", SqlParserPos.ZERO));
      final SqlNode sqlNode = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("First argument to TIME_FLOOR in PARTITIONED BY clause can only be __time", e.getMessage());
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(__time to ISOYEAR)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectIngestionGranularityInFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.ISOYEAR, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("ISOYEAR is not a valid granularity for ingestion", e.getMessage());
    }

    /**
     * Tests clause like "PARTITIONED BY TIME_FLOOR(__time, 'abc')"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectIngestionGranularityInTimeFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString("abc", SqlParserPos.ZERO));
      final SqlNode sqlNode = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("'abc' is an invalid period string", e.getMessage());
    }
  }

  public static class NonParameterizedTests
  {
    private static final DateTimeZone TZ_LOS_ANGELES = DateTimeZone.forID("America/Los_Angeles");

    @Test
    public void test_parseTimeStampWithTimeZone_timestamp_utc()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP,
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestamp_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP,
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestampWithLocalTimeZone()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestampWithLocalTimeZone_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC).toString(),
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestampWithLocalTimeZone()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC).toString(),
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES).toString(),
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestampWithLocalTimeZone_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES).toString(),
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp_invalid()
    {
      final CalciteContextException e = Assert.assertThrows(
          CalciteContextException.class,
          () -> DruidSqlParserUtils.parseTimeStampWithTimeZone(
              SqlLiteral.createUnknown(
                  SqlTypeName.TIMESTAMP.getSpaceName(),
                  "not a timestamp",
                  SqlParserPos.ZERO
              ),
              DateTimeZone.UTC
          )
      );

      MatcherAssert.assertThat(e, ThrowableCauseMatcher.hasCause(CoreMatchers.instanceOf(CalciteException.class)));
      MatcherAssert.assertThat(
          e,
          ThrowableCauseMatcher.hasCause(ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
              "Illegal TIMESTAMP literal 'not a timestamp'")))
      );
    }
  }
}
