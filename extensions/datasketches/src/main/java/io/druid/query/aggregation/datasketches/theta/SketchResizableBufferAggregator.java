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

package io.druid.query.aggregation.datasketches.theta;

import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.MemoryRequest;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.ResizeFactor;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.ResizableBuffer;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class SketchResizableBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final int numEntries;
  private final ResizeFactor rf;
  private final int initSize;

  public SketchResizableBufferAggregator(ObjectColumnSelector selector, int numEntries, ResizeFactor rf, int initSize)
  {
    this.selector = selector;
    this.numEntries = numEntries;
    this.rf = rf;
    this.initSize = initSize;
  }

  @Override
  public void init(final ResizableBuffer buf, final int[] position)
  {
    buf.getByteBuffer(position[0]).put(position[1], (byte) 0);
    SetOperation.builder().initMemory(getUnionMemory(buf, position)).setResizeFactor(rf).build(numEntries, Family.UNION);
  }

  @Override
  public void aggregate(ResizableBuffer buf, int[] position)
  {
    Object update = selector.get();
    if (update == null) {
      return;
    }

    Union union = getUnion(buf, position);
    SketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ResizableBuffer buf, int[] position)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return getUnion(buf, position).getResult(true, null);
  }

  private Union getUnion(ResizableBuffer buf, int[] position)
  {
    return (Union) SetOperation.wrap(getUnionMemory(buf, position));
  }

  /*
    Data is stored in following format on the buffer provided from Druid.

    case-1:
    0th byte = 0 means subsequence bytes contain serialized Union object

    case-2:
    0th byte = 1 means aggregator was resized and this position is only a reference
    next 4 bytes contain size of next ByteBuffer where actual data is stored
    next 4 bytes contain index of next ByteBuffer where actual data is stored
    next 4 bytes contain offset in the ByteBuffer where actual data is stored

 */
  private Memory getUnionMemory(final ResizableBuffer buf, final int[] position)
  {
    ByteBuffer bb = buf.getByteBuffer(position[0]);
    int offset = position[1];
    int size = initSize;

    byte b = bb.get(offset);
    while (b != 0) {
      size = bb.getInt(offset + 1);
      int nextBBIndex = bb.getInt(offset + 5);
      offset = bb.getInt(offset + 9);
      bb = buf.getByteBuffer(nextBBIndex);
      b = bb.get(offset);
    }

    final int theOffset = offset;
    final ByteBuffer theBB = bb;
    final int theSize = size;

    return new MemoryRegion(new NativeMemory(theBB), theOffset + 1, theSize - 1)
    {
      @Override
      public MemoryRequest getMemoryRequest()
      {
        return new MemoryRequest()
        {
          // note that it is assumed that client code would call
          // request(long capacityBytes) and then free(oldMem, newMem) immediately after that.

          int[] newPos = null;
          int newSize = -1;

          @Override
          public Memory request(long capacityBytes)
          {

            if (capacityBytes >= Integer.MAX_VALUE) {
              throw new IAE("can't allocated [%s] bytes.", capacityBytes);
            }

            newSize = (int) capacityBytes + 1;
            newPos = buf.allocate(newSize);
            if (newPos == null) {
              throw new ISE("failed to allocated [%s] bytes.", newSize);
            }

            return new MemoryRegion(new NativeMemory(buf.getByteBuffer(newPos[0])), newPos[1]+1, capacityBytes);
          }

          @Override
          public void free(Memory mem)
          {
            throw new ISE("free(oldMem, newMem) is expected to be called");
          }

          @Override
          public void free(Memory memToFree, Memory newMem)
          {
            if (newPos == null) {
              throw new ISE("WTF! no allocation was done but free is invoked.");
            }

            theBB.put(theOffset, (byte) 1); //marker
            theBB.putInt(theOffset + 1, newSize); //size
            theBB.putInt(theOffset + 5, newPos[0]); //BB index
            theBB.putInt(theOffset + 9, newPos[1]); //offset in BB
            newPos = null;
          }
        };
      }
    };
  }

  @Override
  public float getFloat(ResizableBuffer buf, int[] position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ResizableBuffer buf, int[] position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }
}
