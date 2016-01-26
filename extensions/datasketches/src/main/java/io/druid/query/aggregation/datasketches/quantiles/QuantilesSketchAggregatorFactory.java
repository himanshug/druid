/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import com.yahoo.sketches.quantiles.QuantilesSketchBuilder;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class QuantilesSketchAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 16;

  public static final int DEFAULT_MAX_SKETCH_SIZE = 16384;

  protected final String name;
  protected final String fieldName;
  protected final int size;
  private final boolean shouldFinalize;
  private final boolean isInputSketch;

  public static final Comparator<QuantilesSketch> COMPARATOR = new Comparator<QuantilesSketch>()
  {
    @Override
    public int compare(QuantilesSketch o, QuantilesSketch o1)
    {
      return Doubles.compare(o.getN(), o1.getN());
    }
  };

  @JsonCreator
  public QuantilesSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("size") Integer size,
      @JsonProperty("shouldFinalize") Boolean shouldFinalize,
      @JsonProperty("isInputSketch") boolean isInputSketch
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;
    Util.checkIfPowerOf2(this.size, "size");

    this.shouldFinalize = (shouldFinalize == null) ? true : shouldFinalize.booleanValue();
    this.isInputSketch = isInputSketch;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptyQuantilesSketchAggregator(name, size);
    } else {
      return new QuantilesSketchAggregator(name, selector, size);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return new EmptyQuantilesSketchBufferAggregator(size);
    } else {
      return new QuantilesSketchBufferAggregator(selector, size, getMaxIntermediateSize());
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return QuantilesSketchUtils.deserialize(object);
  }

  @Override
  public Comparator<QuantilesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    QuantilesSketch merged = new QuantilesSketchBuilder().setK(size).build();
    mergeQuantileSketch(merged, lhs);
    mergeQuantileSketch(merged, rhs);
    return merged;
  }

  private void mergeQuantileSketch(QuantilesSketch union, Object obj)
  {
    //would need to handle Memory here once off-heap is supported
    if (obj == null) {
      return;
    } else if (obj instanceof QuantilesSketch) {
      union.merge((QuantilesSketch) obj);
    } else {
      throw new IAE("Object of type [%s] can not be merged to quantiles sketch", obj.getClass().getName());
    }
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
  public int getSize()
  {
    return size;
  }

  @JsonProperty
  public boolean getShouldFinalize()
  {
    return shouldFinalize;
  }

  @JsonProperty
  public boolean getIsInputSketch()
  {
    return isInputSketch;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // off-heap implementation of quantiles sketch is not available right now and hence
    // nothing is stored off-heap really.
    return 1;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return new QuantilesSketchBuilder().setK(size).build();
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory>singletonList(
        new QuantilesSketchAggregatorFactory(
            fieldName,
            fieldName,
            size,
            shouldFinalize,
            isInputSketch
        )
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new QuantilesSketchAggregatorFactory(name, name, size, shouldFinalize, false);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof QuantilesSketchAggregatorFactory) {
      QuantilesSketchAggregatorFactory castedOther = (QuantilesSketchAggregatorFactory) other;

      return new QuantilesSketchAggregatorFactory(
          name,
          name,
          Math.max(size, castedOther.size),
          shouldFinalize,
          false
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  /**
   * Finalize the computation on sketch object and returns numEntries in it.
   * sketch.
   *
   * @param object the sketch object
   *
   * @return sketch object
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    if (shouldFinalize) {
      return ((QuantilesSketch) object).getN();
    } else {
      return object;
    }
  }

  @Override
  public String getTypeName()
  {
    if (isInputSketch) {
      return QuantilesSketchModule.QUANTILES_SKETCH_MERGE_AGG;
    } else {
      return QuantilesSketchModule.QUANTILES_SKETCH_BUILD_AGG;
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] nameBytes = StringUtils.toUtf8(name);
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + nameBytes.length + 1 + fieldNameBytes.length + 1 + Ints.BYTES + 2)
                     .put(CACHE_TYPE_ID)
                     .put(nameBytes)
                     .put(AggregatorFactory.STRING_SEPARATOR)
                     .put(fieldNameBytes)
                     .put(AggregatorFactory.STRING_SEPARATOR)
                     .putInt(size)
                     .put(shouldFinalize ? (byte) 1 : (byte) 0)
                     .put(isInputSketch ? (byte) 1 : (byte) 0)
                     .array();
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

    QuantilesSketchAggregatorFactory that = (QuantilesSketchAggregatorFactory) o;

    if (size != that.size) {
      return false;
    }
    if (shouldFinalize != that.shouldFinalize) {
      return false;
    }
    if (isInputSketch != that.isInputSketch) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return fieldName.equals(that.fieldName);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + size;
    result = 31 * result + (shouldFinalize ? 1 : 0);
    result = 31 * result + (isInputSketch ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "QuantilesSketchAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", size=" + size +
           ", shouldFinalize=" + shouldFinalize +
           ", isInputSketch=" + isInputSketch +
           '}';
  }
}
