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

package org.apache.druid.query.scan;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;


@RunWith(Parameterized.class)
public class UnnestScanQueryRunnerTest extends InitializedNullHandlingTest
{
  public static final QuerySegmentSpec I_0112_0114 = ScanQueryRunnerTest.I_0112_0114;
  private static final VirtualColumn EXPR_COLUMN =
      new ExpressionVirtualColumn("expr", "index * 2", ColumnType.LONG, TestExprMacroTable.INSTANCE);
  private static final ScanQueryQueryToolChest TOOL_CHEST = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );
  private static final ScanQueryRunnerFactory FACTORY = new ScanQueryRunnerFactory(
      TOOL_CHEST,
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );
  private final IncrementalIndex index;
  private final boolean legacy;

  public UnnestScanQueryRunnerTest(final IncrementalIndex index, final boolean legacy)
  {
    this.index = index;
    this.legacy = legacy;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    NullHandling.initializeForTests();
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final List<Object[]> constructors = new ArrayList<>();
    constructors.add(new Object[]{rtIndex, true});
    constructors.add(new Object[]{rtIndex, false});
    return constructors;
  }

  private Druids.ScanQueryBuilder newTestUnnestQuery()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.UNNEST_DATA_SOURCE)
                 .columns(Collections.emptyList())
                 .eternityInterval()
                 .limit(3)
                 .legacy(legacy);
  }

  private Druids.ScanQueryBuilder newTestUnnestQueryWithAllowSet()
  {
    List<String> allowList = Arrays.asList("a", "b", "c");
    LinkedHashSet allowSet = new LinkedHashSet(allowList);
    return Druids.newScanQueryBuilder()
                 .dataSource(UnnestDataSource.create(
                     new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                     QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
                     QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                     allowSet
                 ))
                 .columns(Collections.emptyList())
                 .eternityInterval()
                 .limit(3)
                 .legacy(legacy);
  }

  @Test
  public void testScanOnUnnest()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    if (legacy) {
      values = new String[]{
          "2011-01-12T00:00:00.000Z\ta",
          "2011-01-12T00:00:00.000Z\tpreferred",
          "2011-01-12T00:00:00.000Z\tb"
      };
    } else {
      values = new String[]{
          "a",
          "preferred",
          "b"
      };
    }

    final List<List<Map<String, Object>>> events = toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy
        ? Lists.newArrayList(getTimestampName(), QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        : Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerVirtualColumnsUsingSingleColumn()
  {
    ScanQuery query =
        Druids.newScanQueryBuilder()
              .intervals(I_0112_0114)
              .dataSource(UnnestDataSource.create(
                  new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                  "vc",
                  QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                  null
              ))
              .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
              .eternityInterval()
              .legacy(legacy)
              .virtualColumns(
                  new ExpressionVirtualColumn(
                      "vc",
                      "mv_to_array(placementish)",
                      ColumnType.STRING,
                      TestExprMacroTable.INSTANCE
                  )
              )
              .limit(3)
              .build();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );
    Iterable<ScanResultValue> results = vcrunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    if (legacy) {
      values = new String[]{
          "2011-01-12T00:00:00.000Z\ta",
          "2011-01-12T00:00:00.000Z\tpreferred",
          "2011-01-12T00:00:00.000Z\tb"
      };
    } else {
      values = new String[]{
          "a",
          "preferred",
          "b"
      };
    }

    final List<List<Map<String, Object>>> events = toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy
        ? Lists.newArrayList(getTimestampName(), QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        : Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerVirtualColumnsUsingMultipleColumn()
  {
    ScanQuery query =
        Druids.newScanQueryBuilder()
              .intervals(I_0112_0114)
              .dataSource(UnnestDataSource.create(
                  new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                  "vc",
                  QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                  null
              ))
              .columns(QueryRunnerTestHelper.MARKET_DIMENSION, QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
              .eternityInterval()
              .legacy(legacy)
              .virtualColumns(
                  new ExpressionVirtualColumn(
                      "vc",
                      "array(\"market\",\"quality\")",
                      ColumnType.STRING,
                      TestExprMacroTable.INSTANCE
                  )
              )
              .limit(4)
              .build();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = vcrunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.MARKET_DIMENSION,
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          QueryRunnerTestHelper.MARKET_DIMENSION,
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    if (legacy) {
      values = new String[]{
          "2011-01-12T00:00:00.000Z\tspot\tspot",
          "2011-01-12T00:00:00.000Z\tspot\tautomotive",
          "2011-01-12T00:00:00.000Z\tspot\tspot",
          "2011-01-12T00:00:00.000Z\tspot\tbusiness",
          };
    } else {
      values = new String[]{
          "spot\tspot",
          "spot\tautomotive",
          "spot\tspot",
          "spot\tbusiness"
      };
    }

    final List<List<Map<String, Object>>> events = toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy
        ? Lists.newArrayList(
            getTimestampName(),
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        )
        : Lists.newArrayList(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        ),
        0,
        4
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerWithFilter()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    if (legacy) {
      values = new String[]{
          "2011-01-12T00:00:00.000Z\ta",
          "2011-01-12T00:00:00.000Z\tpreferred",
          "2011-01-12T00:00:00.000Z\tb"
      };
    } else {
      values = new String[]{
          "a",
          "preferred",
          "b"
      };
    }

    final List<List<Map<String, Object>>> events = toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy
        ? Lists.newArrayList(getTimestampName(), QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        : Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  @Test
  public void testUnnestRunnerWithOrdering()
  {
    ScanQuery query = newTestUnnestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.TIME_DIMENSION, QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
        .order(ScanQuery.Order.ASCENDING)
        .build();


    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();
    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          ColumnHolder.TIME_COLUMN_NAME,
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    values = new String[]{
        "2011-01-12T00:00:00.000Z\ta",
        "2011-01-12T00:00:00.000Z\tpreferred",
        "2011-01-12T00:00:00.000Z\tb"
    };

    final List<List<Map<String, Object>>> ascendingEvents = toEvents(columnNames, values);
    if (legacy) {
      for (List<Map<String, Object>> batch : ascendingEvents) {
        for (Map<String, Object> event : batch) {
          event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
        }
      }
    } else {
      for (List<Map<String, Object>> batch : ascendingEvents) {
        for (Map<String, Object> event : batch) {
          event.put("__time", (DateTimes.of((String) event.get("__time"))).getMillis());
        }
      }
    }
    List<ScanResultValue> ascendingExpectedResults = toExpected(
        ascendingEvents,
        legacy ?
        Lists.newArrayList(
            QueryRunnerTestHelper.TIME_DIMENSION,
            getTimestampName(),
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        ) :
        Lists.newArrayList(
            QueryRunnerTestHelper.TIME_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
        ),
        0,
        3
    );

    ScanQueryRunnerTest.verify(ascendingExpectedResults, results);
  }

  @Test
  public void testUnnestRunnerNonNullAllowSet()
  {
    ScanQuery query = newTestUnnestQueryWithAllowSet()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .limit(3)
        .build();

    final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        FACTORY,
        new IncrementalIndexSegment(
            index,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    Iterable<ScanResultValue> results = queryRunner.run(QueryPlus.wrap(query)).toList();

    String[] columnNames;
    if (legacy) {
      columnNames = new String[]{
          getTimestampName() + ":TIME",
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    } else {
      columnNames = new String[]{
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST
      };
    }
    String[] values;
    if (legacy) {
      values = new String[]{
          "2011-01-12T00:00:00.000Z\ta",
          "2011-01-12T00:00:00.000Z\tb",
          "2011-01-13T00:00:00.000Z\ta"
      };
    } else {
      values = new String[]{
          "a",
          "b",
          "a"
      };
    }

    final List<List<Map<String, Object>>> events = toEvents(columnNames, values);
    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy
        ? Lists.newArrayList(getTimestampName(), QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        : Collections.singletonList(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST),
        0,
        3
    );
    ScanQueryRunnerTest.verify(expectedResults, results);
  }


  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<String> values = new ArrayList<>();
    for (String[] vSet : valueSet) {
      values.addAll(Arrays.asList(vSet));
    }
    List<List<Map<String, Object>>> events = new ArrayList<>();
    events.add(
        Lists.newArrayList(
            Iterables.transform(
                values,
                input -> {
                  Map<String, Object> event = new HashMap<>();
                  String[] values1 = input.split("\\t");
                  for (int i = 0; i < dimSpecs.length; i++) {
                    if (dimSpecs[i] == null) {
                      continue;
                    }

                    // For testing metrics and virtual columns we have some special handling here, since
                    // they don't appear in the source data.
                    if (dimSpecs[i].equals(EXPR_COLUMN.getOutputName())) {
                      event.put(
                          EXPR_COLUMN.getOutputName(),
                          (double) event.get(QueryRunnerTestHelper.INDEX_METRIC) * 2
                      );
                      continue;
                    } else if (dimSpecs[i].equals("indexMin")) {
                      event.put("indexMin", (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexFloat")) {
                      event.put("indexFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexMaxPlusTen")) {
                      event.put("indexMaxPlusTen", (double) event.get(QueryRunnerTestHelper.INDEX_METRIC) + 10);
                      continue;
                    } else if (dimSpecs[i].equals("indexMinFloat")) {
                      event.put("indexMinFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexMaxFloat")) {
                      event.put("indexMaxFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("quality_uniques")) {
                      final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
                      collector.add(
                          Hashing.murmur3_128()
                                 .hashBytes(StringUtils.toUtf8((String) event.get("quality")))
                                 .asBytes()
                      );
                      event.put("quality_uniques", collector);
                    }

                    if (i >= values1.length) {
                      continue;
                    }

                    String[] specs = dimSpecs[i].split(":");

                    Object eventVal;
                    if (specs.length == 1 || specs[1].equals("STRING")) {
                      eventVal = values1[i];
                    } else if (specs[1].equals("TIME")) {
                      eventVal = toTimestamp(values1[i]);
                    } else if (specs[1].equals("FLOAT")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultFloatValue() : Float.valueOf(values1[i]);
                    } else if (specs[1].equals("DOUBLE")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultDoubleValue() : Double.valueOf(values1[i]);
                    } else if (specs[1].equals("LONG")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultLongValue() : Long.valueOf(values1[i]);
                    } else if (specs[1].equals(("NULL"))) {
                      eventVal = null;
                    } else if (specs[1].equals("STRINGS")) {
                      eventVal = Arrays.asList(values1[i].split("\u0001"));
                    } else {
                      eventVal = values1[i];
                    }

                    event.put(specs[0], eventVal);
                  }
                  return event;
                }
            )
        )
    );
    return events;
  }

  private Object toTimestamp(final String value)
  {
    if (legacy) {
      return DateTimes.of(value);
    } else {
      return DateTimes.of(value).getMillis();
    }
  }

  private String getTimestampName()
  {
    return legacy ? "timestamp" : ColumnHolder.TIME_COLUMN_NAME;
  }

  private List<ScanResultValue> toExpected(
      List<List<Map<String, Object>>> targets,
      List<String> columns,
      final int offset,
      final int limit
  )
  {
    List<ScanResultValue> expected = Lists.newArrayListWithExpectedSize(targets.size());
    for (List<Map<String, Object>> group : targets) {
      List<Map<String, Object>> events = Lists.newArrayListWithExpectedSize(limit);
      int end = Math.min(group.size(), offset + limit);
      if (end == 0) {
        end = group.size();
      }
      events.addAll(group.subList(offset, end));
      expected.add(new ScanResultValue(QueryRunnerTestHelper.SEGMENT_ID.toString(), columns, events));
    }
    return expected;
  }
}
