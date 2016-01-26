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

import com.metamx.common.ISE;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

public class QuantilesSketchAggregator implements Aggregator
{
  private final ObjectColumnSelector selector;
  private final String name;
  private final int size;

  private QuantilesSketch quantilesSketch;

  public QuantilesSketchAggregator(String name, ObjectColumnSelector selector, int size)
  {
    this.name = name;
    this.selector = selector;
    this.size = size;
    quantilesSketch = QuantilesSketch.builder().setK(size).build();
  }

  @Override
  public void aggregate()
  {
    Object update = selector.get();
    if (update == null) {
      return;
    }

    updateQuantilesSketch(quantilesSketch, update);
  }

  @Override
  public void reset()
  {
    quantilesSketch.reset();
  }

  @Override
  public Object get()
  {
    return quantilesSketch;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    quantilesSketch = null;
  }

  static void updateQuantilesSketch(QuantilesSketch quantilesSketch, Object update)
  {
    //would need to handle Memory when off-heap is supported.
    if (update instanceof QuantilesSketch) {
      quantilesSketch.merge((QuantilesSketch) update);
    } else if (update instanceof Number) {
      quantilesSketch.update(((Number) update).doubleValue());
    } else if (update instanceof String) {
      quantilesSketch.update(Double.parseDouble((String) update));
    } else {
      throw new ISE("Illegal type received while quantiles sketch merging [%s]", update.getClass());
    }
  }
}
