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

import java.nio.ByteBuffer;

/**
 */
public abstract class FixedSizeBufferAggregator implements BufferAggregator
{
  @Override
  public void init(ResizableBuffer buff, int[] position)
  {
    init(buff.getByteBuffer(position[0]), position[1]);
  }

  @Override
  public void aggregate(ResizableBuffer buff, int[] position)
  {
    aggregate(buff.getByteBuffer(position[0]), position[1]);
  }

  @Override
  public Object get(ResizableBuffer buff, int[] position)
  {
    return get(buff.getByteBuffer(position[0]), position[1]);
  }

  @Override
  public float getFloat(ResizableBuffer buff, int[] position)
  {
    return getFloat(buff.getByteBuffer(position[0]), position[1]);
  }

  @Override
  public long getLong(ResizableBuffer buff, int[] position)
  {
    return getLong(buff.getByteBuffer(position[0]), position[1]);
  }

  public abstract void init(ByteBuffer buf, int position);

  public abstract void aggregate(ByteBuffer buf, int position);

  public abstract Object get(ByteBuffer buf, int position);

  public abstract float getFloat(ByteBuffer buf, int position);

  public abstract long getLong(ByteBuffer buf, int position);
}
