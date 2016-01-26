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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class CustomSplitsHistogramPostAggregator implements PostAggregator
{

  private final String name;
  private final double[] splits;
  private final PostAggregator field;

  @JsonCreator
  public CustomSplitsHistogramPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("splits") double[] splits,
      @JsonProperty("field") PostAggregator field

  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    Preconditions.checkArgument(splits != null && splits.length > 0, "null/empty splits");
    this.splits = splits;
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
    long n = sketch.getN();
    double[] result = sketch.getPMF(splits);
    for (int i = 0; i < result.length; i++) {
      result[i] *= n;
    }
    return result;
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
  public double[] getSplits()
  {
    return splits;
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

    CustomSplitsHistogramPostAggregator that = (CustomSplitsHistogramPostAggregator) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (!Arrays.equals(splits, that.splits)) {
      return false;
    }
    return !(field != null ? !field.equals(that.field) : that.field != null);

  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (splits != null ? Arrays.hashCode(splits) : 0);
    result = 31 * result + (field != null ? field.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "HistogramPostAggregator{" +
           "name='" + name + '\'' +
           ", fractions=" + Arrays.toString(splits) +
           ", field=" + field +
           '}';
  }
}
