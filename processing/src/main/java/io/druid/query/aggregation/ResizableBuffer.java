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

package io.druid.query.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.metamx.common.IAE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResizableBuffer
{
  private final StupidPool<ByteBuffer> bufferPool;
  private final List<ResourceHolder<ByteBuffer>> buffers;

  private int currOffset;

  private final int numBuffersToUse;
  private final int singleBufferCapacity;

  public ResizableBuffer(final ByteBuffer bb)
  {
    this(
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return bb;
              }
            }
        ),
        1
    );
  }

  public ResizableBuffer(StupidPool<ByteBuffer> bufferPool, int numBuffersToUse)
  {
    Preconditions.checkArgument(numBuffersToUse > 0, "numBuffersToUse must be greater than 0.");
    this.numBuffersToUse = numBuffersToUse;
    this.bufferPool = bufferPool;

    this.buffers = new ArrayList<>();

    this.currOffset = 0;
    this.buffers.add(this.bufferPool.take());
    this.singleBufferCapacity = this.buffers.get(0).get().capacity();
  }

  public ByteBuffer getByteBuffer(int index)
  {
    return buffers.get(index).get();
  }

  /**
   * Returns int array of size 2
   *   first element is index in the buffers list for the ByteBuffer on which allocation is done.
   *   second element is offset in the ByteBuffer where space is reserved for this allocation.
   *
   * Returns null if allocation fails.
   */
  public int[] allocate(int capacity)
  {
    if (capacity > singleBufferCapacity) {
      throw new IAE(
          "can't allocate [%s] which is more than single buffer capacity [%s]",
          capacity,
          singleBufferCapacity
      );
    }

    if (currOffset + capacity <= singleBufferCapacity) {
      int[] result = new int[]{buffers.size() - 1, currOffset};
      currOffset += capacity;
      return result;
    } else if (buffers.size() < numBuffersToUse) {
      buffers.add(bufferPool.take());
      currOffset = capacity;
      return new int[]{buffers.size() - 1, 0};
    } else {
      return null;
    }
  }

  public int remainingCapacityInCurrentBuffer()
  {
    return singleBufferCapacity - currOffset;
  }

  public void close()
  {
    RuntimeException ex = null;

    for (ResourceHolder<ByteBuffer> holder : buffers) {
      try {
        holder.close();
      }
      catch (Exception e) {
        if (ex == null) {
          ex = new RuntimeException("failed to close.");
        }
        ex.addSuppressed(e);
      }
    }

    buffers.clear();

    if (ex != null) {
      throw ex;
    }
  }

  public void reset()
  {
    close();
    currOffset = 0;
    buffers.add(this.bufferPool.take());
  }
}
