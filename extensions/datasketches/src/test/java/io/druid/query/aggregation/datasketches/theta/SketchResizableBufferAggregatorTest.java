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

import com.yahoo.sketches.theta.ResizeFactor;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import io.druid.query.aggregation.ResizableBuffer;
import io.druid.segment.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class SketchResizableBufferAggregatorTest
{
  @Test
  public void testResizing()
  {
    int k = 1024;
    ResizeFactor rf = ResizeFactor.X8;
    int initSize = Math.max(1 + (SetOperation.getMaxUnionBytes(k) / rf.getValue()), 16);

    SketchResizableBufferAggregator aggregator = new SketchResizableBufferAggregator(
        new ObjectColumnSelector()
        {
          int i = 0;

          @Override
          public Class classOfObject()
          {
            return String.class;
          }

          @Override
          public Object get()
          {
            return "v" + i++;
          }
        },
        k,
        rf,
        initSize
    );

    ResizableBuffer resizableBuffer = new ResizableBuffer(ByteBuffer.allocate(1024*1024*32));

    int[] pos = resizableBuffer.allocate(initSize);
    aggregator.init(resizableBuffer, pos);

    for (int i = 0; i < 10000; i++) {
      aggregator.aggregate(resizableBuffer, pos);
    }

    Sketch sketch = (Sketch) aggregator.get(resizableBuffer, pos);
    Assert.assertEquals(9623, sketch.getEstimate(), 0.1);
  }
}
