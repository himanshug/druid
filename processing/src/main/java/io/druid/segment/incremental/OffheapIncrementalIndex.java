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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.ResizableBuffer;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private final ResizableBuffer resizableBuffer;
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;

  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  protected final int maxRowCount;

  private volatile Map<String, ColumnSelectorFactory> selectors;

  private volatile int[] aggOffsetInBuffer;
  private volatile int aggregatorInitSizesTotal;
  private volatile int aggregatorMaxSizesTotal;

  // reason if this index can't ingest more rows, currently it the limit is specified via number of rows
  // this will be updated so that limit can be specified in terms of memory usage.
  private String outOfRowsReason = null;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions);
    this.maxRowCount = maxRowCount;
    this.facts = new ConcurrentSkipListMap<>(dimsComparator());

    this.resizableBuffer = new ResizableBuffer(bufferPool, Integer.MAX_VALUE);

    //check that stupid pool gives buffers that can hold at least one row's aggregators
    if (this.resizableBuffer.remainingCapacityInCurrentBuffer() < aggregatorMaxSizesTotal) {
      RuntimeException ex = new IAE("bufferPool buffers capacity must be >= [%s]", aggregatorMaxSizesTotal);
      try {
        this.resizableBuffer.close();
      } catch(Exception e){
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        maxRowCount,
        bufferPool
    );
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        true,
        maxRowCount,
        bufferPool
    );
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnheapIncrementalIndex.OnHeapDimDim(lock);
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    selectors = Maps.newHashMap();

    aggOffsetInBuffer = new int[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
          agg,
          rowSupplier,
          deserializeComplexMetrics
      );

      selectors.put(
          agg.getName(),
          new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(columnSelectorFactory)
      );

      if (i == 0) {
        aggOffsetInBuffer[i] = 0;
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i-1] + metrics[i-1].getInitSize();
      }

      aggregatorInitSizesTotal += metrics[i].getInitSize();
      aggregatorMaxSizesTotal += metrics[i].getMaxIntermediateSize();
    }

    return new BufferAggregator[metrics.length];
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    int[] indexAndOffset;

    synchronized (this) {
      final Integer priorIndex = facts.get(key);
      if (null != priorIndex) {
        indexAndOffset = indexAndOffsets.get(priorIndex);
      } else {
        if (metrics.length > 0 && getAggs()[0] == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          rowContainer.set(row);
          for (int i = 0; i < metrics.length; i++) {
            final AggregatorFactory agg = metrics[i];
            getAggs()[i] = agg.factorizeBuffered(
                makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics)
            );
          }
          rowContainer.set(null);
        }

        indexAndOffset = resizableBuffer.allocate(aggregatorInitSizesTotal);
        int[] pos = new int[]{indexAndOffset[0], 0};
        for (int i = 0; i < metrics.length; i++) {
          pos[1] = indexAndOffset[1] + aggOffsetInBuffer[i];
          getAggs()[i].init(resizableBuffer, pos);
        }

        // Last ditch sanity checks
        if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final Integer rowIndex = indexIncrement.getAndIncrement();

        // note that indexAndOffsets must be updated before facts, because as soon as we update facts
        // concurrent readers get hold of it and might ask for newly added row
        indexAndOffsets.add(indexAndOffset);
        final Integer prev = facts.putIfAbsent(key, rowIndex);
        if (null == prev) {
          numEntries.incrementAndGet();
        } else {
          throw new ISE("WTF! we are in sychronized block.");
        }
      }
    }

    rowContainer.set(row);

    int[] pos = new int[]{indexAndOffset[0], 0};
    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        try {
          pos[1] = indexAndOffset[1] + aggOffsetInBuffer[i];
          agg.aggregate(resizableBuffer, pos);
        } catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw e;
          }
        }
      }
    }
    rowContainer.set(null);
    return numEntries.get();
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = String.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    return agg.get(resizableBuffer, new int[]{indexAndOffset[0], indexAndOffset[1] + aggOffsetInBuffer[aggPosition]});
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    return agg.getFloat(resizableBuffer, new int[]{indexAndOffset[0], indexAndOffset[1] + aggOffsetInBuffer[aggOffset]});
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    return agg.getLong(resizableBuffer, new int[]{indexAndOffset[0], indexAndOffset[1] + aggOffsetInBuffer[aggOffset]});
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    return agg.get(resizableBuffer, new int[]{indexAndOffset[0], indexAndOffset[1] + aggOffsetInBuffer[aggOffset]});
  }

  /**
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    facts.clear();
    indexAndOffsets.clear();

    if (selectors != null) {
      selectors.clear();
    }

    resizableBuffer.close();
  }
}
