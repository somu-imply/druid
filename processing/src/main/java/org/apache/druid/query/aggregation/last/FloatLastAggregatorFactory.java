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
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
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

public class FloatLastAggregatorFactory extends AggregatorFactory
{
  private static final Aggregator NIL_AGGREGATOR = new FloatLastAggregator(
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

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new FloatLastBufferAggregator(
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
  public FloatLastAggregatorFactory(
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
    final BaseFloatColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new FloatLastAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BaseFloatColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new FloatLastBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector
      );
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory
  )
  {
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
    VectorValueSelector valueSelector = columnSelectorFactory.makeValueSelector(fieldName);

    VectorValueSelector timeSelector = columnSelectorFactory.makeValueSelector(
        timeColumn);
    if (capabilities == null || capabilities.isNumeric()) {
      return new FloatLastVectorAggregator(timeSelector, valueSelector);
    } else {
      return NumericNilVectorAggregator.floatNilVectorAggregator();
    }
  }

  @Override
  public Comparator getComparator()
  {
    return FloatFirstAggregatorFactory.VALUE_COMPARATOR;
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
    Long leftTime = ((SerializablePair<Long, Float>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Float>) rhs).lhs;
    if (leftTime >= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("FloatLastAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new FloatLastAggregatorFactory(name, name, timeColumn)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        ColumnValueSelector<SerializablePair<Long, Float>> selector = metricFactory.makeColumnValueSelector(name);
        return new FloatLastAggregator(null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Float> pair = selector.getObject();
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
        ColumnValueSelector<SerializablePair<Long, Float>> selector = metricFactory.makeColumnValueSelector(name);
        return new FloatLastBufferAggregator(null, null)
        {
          @Override
          public void putValue(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Float> pair = selector.getObject();
            buf.putFloat(position, pair.rhs);
          }

          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Float> pair = selector.getObject();
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
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).floatValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePair<Long, Float>) object).rhs;
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
    return new CacheKeyBuilder(AggregatorUtil.FLOAT_LAST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(timeColumn)
        .appendByte((byte) 0xff)
        .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    // if we don't pretend to be a primitive, group by v1 gets sad and doesn't work because no complex type serde
    return ColumnType.FLOAT;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.FLOAT;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Float.BYTES;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new FloatLastAggregatorFactory(newName, getFieldName(), getTimeColumn());
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

    FloatLastAggregatorFactory that = (FloatLastAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && timeColumn.equals(that.timeColumn) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return "FloatLastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", timeColumn='" + timeColumn + '\'' +
           '}';
  }
}
