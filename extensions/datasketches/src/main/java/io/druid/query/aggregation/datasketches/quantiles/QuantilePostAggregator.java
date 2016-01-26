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
import com.google.common.collect.Sets;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class QuantilePostAggregator implements PostAggregator
{
  private final String name;
  private final double fraction;
  private final PostAggregator field;

  @JsonCreator
  public QuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fraction") double fraction,
      @JsonProperty("field") PostAggregator field

  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.fraction = fraction;
    this.field = Preconditions.checkNotNull(field, "field is null");
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newHashSet();
    dependentFields.addAll(field.getDependentFields());
    return dependentFields;
  }

  @Override
  public Comparator<QuantilesSketch> getComparator()
  {
    return QuantilesSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    QuantilesSketch sketch = (QuantilesSketch) field.compute(combinedAggregators);
    return sketch.getQuantile(fraction);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double getFraction()
  {
    return fraction;
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

    QuantilePostAggregator that = (QuantilePostAggregator) o;

    if (Double.compare(that.fraction, fraction) != 0) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return field.equals(that.field);

  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    result = name.hashCode();
    temp = Double.doubleToLongBits(fraction);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + field.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "QuantilePostAggregator{" +
           "name='" + name + '\'' +
           ", fraction=" + fraction +
           ", field=" + field +
           '}';
  }
}
