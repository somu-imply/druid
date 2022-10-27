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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class UnnestStorageAdapterTest extends InitializedNullHandlingTest
{
  private static Closer CLOSER;
  private static IncrementalIndex INCREMENTAL_INDEX;
  private static IncrementalIndexStorageAdapter INCREMENTAL_INDEX_STORAGE_ADAPTER;
  private static UnnestStorageAdapter UNNEST_STORAGE_ADAPTER;
  private static List<StorageAdapter> ADAPTERS;
  private static String COLUMNNAME = "multi-string1";
  private static String OUTPUT_COLUMN_NAME = "unnested-multi-string1";
  private static boolean DEDUP = false;

  @BeforeClass
  public static void setup()
  {
    CLOSER = Closer.create();
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();
    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());

    final int numRows = 10_000;
    INCREMENTAL_INDEX = CLOSER.register(
        segmentGenerator.generateIncrementalIndex(dataSegment, schemaInfo, Granularities.HOUR, numRows)
    );
    INCREMENTAL_INDEX_STORAGE_ADAPTER = new IncrementalIndexStorageAdapter(INCREMENTAL_INDEX);
    UNNEST_STORAGE_ADAPTER = new UnnestStorageAdapter(
        INCREMENTAL_INDEX_STORAGE_ADAPTER,
        COLUMNNAME,
        OUTPUT_COLUMN_NAME,
        DEDUP
    );
    ADAPTERS = ImmutableList.of(UNNEST_STORAGE_ADAPTER);
  }

  @AfterClass
  public static void teardown()
  {
    CloseableUtils.closeAndSuppressExceptions(CLOSER, throwable -> {
    });
  }

  @Test
  public void test_unnest_adapter_methods()
  {
    String colName = "multi-string1";
    for (StorageAdapter adapter : ADAPTERS) {
      Assert.assertEquals(
          DateTimes.of("2000-01-01T23:00:00.000Z"),
          adapter.getMaxTime()
      );
      Assert.assertEquals(
          DateTimes.of("2000-01-01T00:00:00.000Z"),
          adapter.getMinTime()
      );
      adapter.getColumnCapabilities(colName);
      Assert.assertEquals(adapter.getNumRows(), 0);
      Assert.assertNotNull(adapter.getMetadata());
      Assert.assertEquals(
          DateTimes.of("2000-01-01T23:59:59.999Z"),
          adapter.getMaxIngestedEventTime()
      );
      Assert.assertEquals(
          adapter.getColumnCapabilities(colName).toColumnType(),
          INCREMENTAL_INDEX_STORAGE_ADAPTER.getColumnCapabilities(colName).toColumnType()
      );
      Assert.assertEquals(UNNEST_STORAGE_ADAPTER.getDimensionToUnnest(), colName);
    }
  }

  @Test
  public void test_unnest_adapters()
  {
    final String columnName = "multi-string1";

    for (StorageAdapter adapter : ADAPTERS) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          null,
          adapter.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );


      cursorSequence.accumulate(null, (accumulated, cursor) -> {
        ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();

        DimensionSelector dimSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
        ColumnValueSelector valueSelector = factory.makeColumnValueSelector(columnName);


        while (!cursor.isDone()) {
          Object dimSelectorVal = dimSelector.getObject();
          Object valueSelectorVal = valueSelector.getObject();
          if (dimSelectorVal == null) {
            Assert.assertNull(dimSelectorVal);
          } else if (valueSelectorVal == null) {
            Assert.assertNull(valueSelectorVal);
          }
          cursor.advance();
        }
        return null;
      });
    }
  }
}
