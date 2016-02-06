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
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnDiskIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private final StupidPool<ByteBuffer> bufferPool;

  private final List<ResourceHolder<ByteBuffer>> aggBuffers = new ArrayList<>();
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final DB factsDb;
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;

  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  protected final int maxRowCount;

  private volatile Map<String, ColumnSelectorFactory> selectors;

  //given a ByteBuffer and an offset where all aggregates for a row are stored
  //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
  //is stored
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;

  private String outOfRowsReason = null;

  public OnDiskIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);
    this.maxRowCount = maxRowCount;
    this.bufferPool = bufferPool;

    //See http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html
    final DBMaker dbMaker = DBMaker.newTempFileDB()
                                   .deleteFilesAfterClose()
        .commitFileSyncDisable()
        .cacheSize(128)
//      .sizeLimit(Integer.MAX_VALUE)
        .transactionDisable()
        .asyncWriteEnable()
        .mmapFileEnableIfSupported();

    this.factsDb = dbMaker.make();
    final TimeAndDimsSerializer timeAndDimsSerializer = new TimeAndDimsSerializer(this);
    this.facts = factsDb.createTreeMap("__facts" + UUID.randomUUID())
                        .keySerializer(timeAndDimsSerializer)
                        .comparator(timeAndDimsSerializer.getComparator())
                        .valueSerializer(Serializer.INTEGER)
                        .make();


    //check that stupid pool gives buffers that can hold at least one row's aggregators
    ResourceHolder<ByteBuffer> bb = bufferPool.take();
    if (bb.get().capacity() < aggsTotalSize) {
      RuntimeException ex = new IAE("bufferPool buffers capacity must be >= [%s]", aggsTotalSize);
      try {
        bb.close();
      } catch(IOException ioe){
        ex.addSuppressed(ioe);
      }
      throw ex;
    }
    aggBuffers.add(bb);
  }

  public OnDiskIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
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
        maxRowCount,
        bufferPool
    );
  }

  public OnDiskIncrementalIndex(
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

    BufferAggregator[] aggregators = new BufferAggregator[metrics.length];

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

      aggregators[i] = agg.factorizeBuffered(columnSelectorFactory);
      if (i == 0) {
        aggOffsetInBuffer[i] = 0;
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i-1] + metrics[i-1].getMaxIntermediateSize();
      }
    }

    aggsTotalSize = aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();

    return aggregators;
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    ByteBuffer aggBuffer;
    int bufferIndex;
    int bufferOffset;

    synchronized (this) {
      final Integer priorIndex = facts.get(key);
      if (null != priorIndex) {
        final int[] indexAndOffset = indexAndOffsets.get(priorIndex);
        bufferIndex = indexAndOffset[0];
        bufferOffset = indexAndOffset[1];
        aggBuffer = aggBuffers.get(bufferIndex).get();
      } else {
        bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.isEmpty() ? null : aggBuffers.get(aggBuffers.size() - 1).get();
        int[] lastAggregatorsIndexAndOffset = indexAndOffsets.isEmpty()
                                              ? null
                                              : indexAndOffsets.get(indexAndOffsets.size() - 1);

        if (lastAggregatorsIndexAndOffset != null && lastAggregatorsIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        bufferOffset = aggsTotalSize + (lastAggregatorsIndexAndOffset != null ? lastAggregatorsIndexAndOffset[1] : 0);
        if (lastBuffer != null &&
            lastBuffer.capacity() - bufferOffset >= aggsTotalSize) {
          aggBuffer = lastBuffer;
        } else {
          ResourceHolder<ByteBuffer> bb = bufferPool.take();
          aggBuffers.add(bb);
          bufferIndex = aggBuffers.size() - 1;
          bufferOffset = 0;
          aggBuffer = bb.get();
        }

        for (int i = 0; i < metrics.length; i++) {
          getAggs()[i].init(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        }

        // Last ditch sanity checks
        if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final Integer rowIndex = indexIncrement.getAndIncrement();
        final Integer prev = facts.putIfAbsent(key, rowIndex);
        if (null == prev) {
          numEntries.incrementAndGet();
          indexAndOffsets.add(new int[]{bufferIndex, bufferOffset});
        } else {
          throw new ISE("WTF! we are in sychronized block.");
        }
      }
    }

    rowContainer.set(row);

    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        agg.aggregate(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
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
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggPosition]);
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.getFloat(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.getLong(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  /**
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    RuntimeException ex = null;

    //note that it is important to first clear mapDB stuff as it depends on dimLookups
    //in the comparator which should be available.
    try {
      facts.clear();
      factsDb.close();
    } catch (Exception e) {
      if (ex == null) {
        ex = Throwables.propagate(e);
      } else {
        ex.addSuppressed(e);
      }
    }

    super.close();

    indexAndOffsets.clear();

    if (selectors != null) {
      selectors.clear();
    }


    for (ResourceHolder<ByteBuffer> buffHolder : aggBuffers) {
      try {
        buffHolder.close();
      } catch(IOException ioe) {
        if (ex == null) {
          ex = Throwables.propagate(ioe);
        } else {
          ex.addSuppressed(ioe);
        }
      }
    }
    aggBuffers.clear();

    if (ex != null) {
      throw ex;
    }
  }

  private static class TimeAndDimsSerializer extends BTreeKeySerializer<TimeAndDims> implements Serializable
  {
    private static final int[] EMPTY_INT_ARRAY = new int[0];

    private final Comparator<TimeAndDims> comparator;

    TimeAndDimsSerializer(final OnDiskIncrementalIndex tHis)
    {
      this.comparator = new OnDiskTimeAndDimsComparator(
          new Supplier<List<DimDim>>()
          {
            @Override
            public List<DimDim> get()
            {
              return tHis.dimValues;
            }
          }
      );
    }

    //TODO: write VSizeIndexed based serializer that will reduce the key size.
    //also do the null check etc.
    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException
    {
      for (int i = start; i < end; i++) {
        TimeAndDims timeAndDim = (TimeAndDims) keys[i];
        out.writeLong(timeAndDim.getTimestamp());
        out.writeInt(timeAndDim.getDims().length);
        for (int[] dims : timeAndDim.getDims()) {
          if (dims == null) {
            writeArr(EMPTY_INT_ARRAY, out);
          } else {
            writeArr(dims, out);
          }
        }
      }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException
    {
      Object[] ret = new Object[size];
      for (int i = start; i < end; i++) {
        final long timeStamp = in.readLong();
        final int[][] dims = new int[in.readInt()][]; //TODO: optimize for length 0

        for (int j = 0; j < dims.length; j++) {
          dims[j] = readArr(in);
        }
        ret[i] = new TimeAndDims(timeStamp, dims);
      }

      return ret;
    }

    @Override
    public Comparator<TimeAndDims> getComparator()
    {
      return comparator;
    }

    private void writeArr(int[] value, DataOutput out) throws IOException
    {
      out.writeInt(value.length);
      for (int v : value) {
        out.writeInt(v);
      }
    }

    private int[] readArr(DataInput in) throws IOException
    {
      int len = in.readInt();
      if (len == 0) {
        return EMPTY_INT_ARRAY;
      } else {
        int[] result = new int[len];
        for (int i = 0; i < len; i++) {
          result[i] = in.readInt();
        }
        return result;
      }
    }
  }

  private static class OnDiskTimeAndDimsComparator implements Comparator<TimeAndDims>, Serializable
  {
    private transient Supplier<List<DimDim>> dimDimsSupplier;

    OnDiskTimeAndDimsComparator(Supplier<List<DimDim>> dimDimsSupplier)
    {
      this.dimDimsSupplier = dimDimsSupplier;
    }

    @Override
    public int compare(TimeAndDims o1, TimeAndDims o2)
    {
      return new TimeAndDimsComp(dimDimsSupplier.get()).compare(o1, o2);
    }
  }
}
