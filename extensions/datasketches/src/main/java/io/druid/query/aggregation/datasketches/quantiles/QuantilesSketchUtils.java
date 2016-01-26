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

import com.google.common.base.Charsets;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import org.apache.commons.codec.binary.Base64;

public class QuantilesSketchUtils
{
  public static QuantilesSketch deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof QuantilesSketch) {
      return (QuantilesSketch) serializedSketch;
    }

    throw new IllegalStateException(
        "Object is not of a type that can deserialize to sketch: "
        + serializedSketch.getClass()
    );
  }

  public static QuantilesSketch deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(
        Base64.decodeBase64(
            str.getBytes(Charsets.UTF_8)
        )
    );
  }

  public static QuantilesSketch deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(new NativeMemory(data));
  }

  public static QuantilesSketch deserializeFromMemory(Memory mem)
  {
    //should support "wrap" once off heap is supported
    return QuantilesSketch.heapify(mem);
  }
}
