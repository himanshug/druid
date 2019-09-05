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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ExtractionFnVirtualColumn implements VirtualColumn
{
  private final String name;
  private final String fieldName;
  private final ExtractionFn extractionFn;

  public ExtractionFnVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this.name = Preconditions.checkNotNull(name, "null name");
    this.fieldName = Preconditions.checkNotNull(name, "null fieldName");
    this.extractionFn = Preconditions.checkNotNull(extractionFn, "null extractionFn");
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec, ColumnSelectorFactory factory
  )
  {
    ColumnCapabilities capabilities = factory.getColumnCapabilities(fieldName);

    ValueType valueType = capabilities == null ? ValueType.STRING : capabilities.getType();
    if (valueType.isNumeric()) {
      return valueType.makeNumericWrappingDimensionSelector(
          makeColumnValueSelector(dimensionSpec.getDimension(), factory),
          extractionFn
      );
    } else {
      DimensionSelector baseDimensionSelector = factory.makeDimensionSelector(dimensionSpec);

      if (baseDimensionSelector == null) {
        return DimensionSelector.constant(extractionFn.apply(null));
      }

      return wrapWithExtractionFn(baseDimensionSelector);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName, ColumnSelectorFactory factory
  )
  {
    return makeDimensionSelector(
        new DefaultDimensionSpec(columnName, columnName, ValueType.STRING),
        factory
    );
  }

  @Override
  public ColumnCapabilities capabilities(String columnName, Function<String, ColumnCapabilities> columnCapabilities)
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(ValueType.STRING);

    ColumnCapabilities fieldCapabilities = columnCapabilities.apply(fieldName);
    if (fieldCapabilities != null && fieldCapabilities.hasMultipleValues()) {
      capabilities = capabilities.setHasMultipleValues(true);
    }

    return capabilities;
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXTRACTION_FN)
        .appendString(name)
        .appendCacheable(extractionFn)
        .build();
  }

  private DimensionSelector wrapWithExtractionFn(DimensionSelector delegate)
  {
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return delegate.getRow();
      }

      @Override
      public int getValueCardinality()
      {
        if (extractionFn.getExtractionType() == ExtractionFn.ExtractionType.ONE_TO_ONE &&
            extractionFn.preservesOrdering()) {
          return delegate.getValueCardinality();
        } else {
          return CARDINALITY_UNKNOWN;
        }
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return extractionFn.apply(delegate.lookupName(id));
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return defaultGetObject();
      }

      @Override
      public Class<?> classOfObject()
      {
        return String.class;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return false;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", delegate);
      }
    };
  }
}
