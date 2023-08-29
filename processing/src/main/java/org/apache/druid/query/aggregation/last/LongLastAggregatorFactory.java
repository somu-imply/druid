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

package org.apache.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.any.NumericNilVectorAggregator;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LongLastAggregatorFactory extends AggregatorFactory
{
  private static final Aggregator NIL_AGGREGATOR = new LongLastAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance()
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new LongLastBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance()
  )
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };

  private final String fieldName;
  private final String timeColumn;
  private final String name;

  @JsonCreator
  public LongLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("timeColumn") @Nullable final String timeColumn
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    this.name = name;
    this.fieldName = fieldName;
    this.timeColumn = timeColumn == null ? ColumnHolder.TIME_COLUMN_NAME : timeColumn;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final BaseLongColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new LongLastAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BaseLongColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new LongLastBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector
      );
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
    return capabilities != null && capabilities.isNumeric();
  }

  @Override
  public VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory
  )
  {
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
    if (capabilities == null || capabilities.isNumeric()) {
      VectorValueSelector valueSelector = columnSelectorFactory.makeValueSelector(fieldName);
      VectorValueSelector timeSelector = columnSelectorFactory.makeValueSelector(timeColumn);
      return new LongLastVectorAggregator(timeSelector, valueSelector);
    } else {
      return NumericNilVectorAggregator.longNilVectorAggregator();
    }
  }

  @Override
  public Comparator getComparator()
  {
    return LongFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    Long leftTime = ((SerializablePair<Long, Long>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Long>) rhs).lhs;
    if (leftTime >= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("LongLastAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongLastAggregatorFactory(name, name, timeColumn)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final ColumnValueSelector<SerializablePair<Long, Long>> selector = metricFactory.makeColumnValueSelector(name);
        return new LongLastAggregator(null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Long> pair = selector.getObject();
            if (pair.lhs >= lastTime) {
              lastTime = pair.lhs;
              if (pair.rhs != null) {
                lastValue = pair.rhs;
                rhsNull = false;
              } else {
                rhsNull = true;
              }
            }
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ColumnValueSelector<SerializablePair<Long, Long>> selector = metricFactory.makeColumnValueSelector(name);
        return new LongLastBufferAggregator(null, null)
        {
          @Override
          public void putValue(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Long> pair = selector.getObject();
            buf.putLong(position, pair.rhs);
          }

          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Long> pair = selector.getObject();
            long lastTime = buf.getLong(position);
            if (pair.lhs >= lastTime) {
              if (pair.rhs != null) {
                updateTimeWithValue(buf, position, pair.lhs);
              } else {
                updateTimeWithNull(buf, position, pair.lhs);
              }
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      }
    };
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (map.get("rhs") == null) {
      return new SerializablePair<>(((Number) map.get("lhs")).longValue(), null);
    }
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).longValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePair<Long, Long>) object).rhs;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getTimeColumn()
  {
    return timeColumn;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(timeColumn, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.LONG_LAST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(timeColumn)
        .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    // if we don't pretend to be a primitive, group by v1 gets sad and doesn't work because no complex type serde
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Long.BYTES;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new LongLastAggregatorFactory(newName, getFieldName(), getTimeColumn());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongLastAggregatorFactory that = (LongLastAggregatorFactory) o;

    return name.equals(that.name) && timeColumn.equals(that.timeColumn) && fieldName.equals(that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, timeColumn);
  }

  @Override
  public String toString()
  {
    return "LongLastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", timeColumn='" + timeColumn + '\'' +
           '}';
  }
}
