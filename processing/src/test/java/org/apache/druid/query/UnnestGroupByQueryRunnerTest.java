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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@RunWith(Parameterized.class)
public class UnnestGroupByQueryRunnerTest extends InitializedNullHandlingTest
{
  public static final ObjectMapper DEFAULT_MAPPER = TestHelper.makeSmileMapper();
  public static final DruidProcessingConfig DEFAULT_PROCESSING_CONFIG = new DruidProcessingConfig()
  {
    @Override
    public String getFormatString()
    {
      return null;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return 10 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      // Some tests need two buffers for testing nested groupBy (simulating two levels of merging).
      // Some tests need more buffers for parallel combine (testMergedPostAggHavingSpec).
      return 4;
    }

    @Override
    public int getNumThreads()
    {
      return 2;
    }
  };

  private static TestGroupByBuffers BUFFER_POOLS = null;

  private final QueryRunner<ResultRow> runner;
  private final QueryRunner<ResultRow> originalRunner;
  private final GroupByQueryRunnerFactory factory;
  private final GroupByQueryConfig config;
  private final boolean vectorize;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public UnnestGroupByQueryRunnerTest(
      String testName,
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner,
      boolean vectorize
  )
  {
    this.config = config;
    this.factory = factory;
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
    this.originalRunner = runner;
    String runnerName = runner.toString();
    this.vectorize = vectorize;
  }

  public static List<GroupByQueryConfig> testConfigs()
  {

    final GroupByQueryConfig v2Config = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        // Small initial table to force some growing.
        return 4;
      }

      @Override
      public String toString()
      {
        return "v2";
      }
    };
    final GroupByQueryConfig v2SmallBufferConfig = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public int getBufferGrouperMaxSize()
      {
        return 2;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(10L * 1024 * 1024);
      }

      @Override
      public String toString()
      {
        return "v2SmallBuffer";
      }
    };
    final GroupByQueryConfig v2SmallDictionaryConfig = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(10L * 1024 * 1024);
      }

      @Override
      public String toString()
      {
        return "v2SmallDictionary";
      }
    };
    final GroupByQueryConfig v2ParallelCombineConfig = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public int getNumParallelCombineThreads()
      {
        return DEFAULT_PROCESSING_CONFIG.getNumThreads();
      }

      @Override
      public String toString()
      {
        return "v2ParallelCombine";
      }
    };


    return ImmutableList.of(
        v2Config,
        v2SmallBufferConfig,
        v2SmallDictionaryConfig,
        v2ParallelCombineConfig
    );
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(DEFAULT_MAPPER, config, bufferPools, DEFAULT_PROCESSING_CONFIG);
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(mapper, config, bufferPools, DEFAULT_PROCESSING_CONFIG);
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools,
      final DruidProcessingConfig processingConfig
  )
  {
    if (bufferPools.getBufferSize() != processingConfig.intermediateComputeSizeBytes()) {
      throw new ISE(
          "Provided buffer size [%,d] does not match configured size [%,d]",
          bufferPools.getBufferSize(),
          processingConfig.intermediateComputeSizeBytes()
      );
    }
    if (bufferPools.getNumMergeBuffers() != processingConfig.getNumMergeBuffers()) {
      throw new ISE(
          "Provided merge buffer count [%,d] does not match configured count [%,d]",
          bufferPools.getNumMergeBuffers(),
          processingConfig.getNumMergeBuffers()
      );
    }
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPools.getProcessingPool()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new GroupByStrategyV2(
            processingConfig,
            configSupplier,
            bufferPools.getProcessingPool(),
            bufferPools.getMergePool(),
            TestHelper.makeJsonMapper(),
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(strategySelector);
    return new GroupByQueryRunnerFactory(strategySelector, toolChest);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    NullHandling.initializeForTests();
    setUpClass();

    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      final GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(config, BUFFER_POOLS);
      for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeUnnestQueryRunners(
          factory,
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
          QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
          null
      )) {
        for (boolean vectorize : ImmutableList.of(false)) {
          final String testName = StringUtils.format("config=%s, runner=%s, vectorize=%s", config, runner, vectorize);

          // Add vectorization tests for any indexes that support it.
          if (!vectorize ||
              (QueryRunnerTestHelper.isTestRunnerVectorizable(runner) &&
               config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2))) {
            constructors.add(new Object[]{testName, config, factory, runner, vectorize});
          }
        }
      }
    }

    return constructors;
  }

  @BeforeClass
  public static void setUpClass()
  {
    if (BUFFER_POOLS == null) {
      BUFFER_POOLS = TestGroupByBuffers.createDefault();
    }
  }

  @AfterClass
  public static void tearDownClass()
  {
    BUFFER_POOLS.close();
    BUFFER_POOLS = null;
  }

  private static ResultRow makeRow(final GroupByQuery query, final String timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  private static ResultRow makeRow(final GroupByQuery query, final DateTime timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  private static List<ResultRow> makeRows(
      final GroupByQuery query,
      final String[] columnNames,
      final Object[]... values
  )
  {
    return GroupByQueryRunnerTestHelper.createExpectedRows(query, columnNames, values);
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            null
        ))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            270L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            236L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            316L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            240L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            5740L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            242L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5800L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            156L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            238L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            294L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            224L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            332L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            226L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            4894L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            228L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5010L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            194L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            252L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnMissingColumn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            null
        ))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("nonexistent0", "alias0"),
            new ExtractionDimensionSpec("nonexistent1", "alias1", new StringFormatExtractionFn("foo"))
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", null,
            "alias1", "foo",
            "rows", 52L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "missing-column");
  }

  @Test
  public void testGroupByOnUnnestedColumn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            null
        ))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0")
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    // Total rows should add up to 26 * 2 = 52
    // 26 rows and each has 2 entries in the column to be unnested
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "a",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "b",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "e",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "h",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "m",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "n",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "p",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "preferred",
            "rows", 26L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "t",
            "rows", 4L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "hroupBy-on-unnested-column");
  }

  /**
   * Use this method instead of makeQueryBuilder() to make sure the context is set properly. Also, avoid
   * setContext in tests. Only use overrideContext.
   */
  private GroupByQuery.Builder makeQueryBuilder()
  {
    return GroupByQuery.builder().overrideContext(makeContext());
  }

  /**
   * Use this method instead of makeQueryBuilder() to make sure the context is set properly. Also, avoid
   * setContext in tests. Only use overrideContext.
   */
  private GroupByQuery.Builder makeQueryBuilder(final GroupByQuery query)
  {
    return new GroupByQuery.Builder(query).overrideContext(makeContext());
  }

  private Map<String, Object> makeContext()
  {
    return ImmutableMap.<String, Object>builder()
                       .put(QueryContexts.VECTORIZE_KEY, vectorize ? "force" : "false")
                       .put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize ? "force" : "false")
                       .put("vectorSize", 16) // Small vector size to ensure we use more than one.
                       .build();
  }

  private void cannotVectorize()
  {
    if (vectorize && config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("Cannot vectorize!");
    }
  }
}
