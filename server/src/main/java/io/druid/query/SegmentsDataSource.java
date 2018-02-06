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

package io.druid.query;

import com.google.common.collect.Sets;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.java.util.common.Pair;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 */
public interface SegmentsDataSource extends DataSource
{
  Set<ServerToSegment> getSegments(List<Interval> intervals, TimelineServerView serverView);
}

class UnionDataSource2 implements SegmentsDataSource
{
  List<TableDataSource> dataSources;

  @Override
  public Set<ServerToSegment> getSegments(List<Interval> intervals, TimelineServerView serverView)
  {
    final Set<ServerToSegment> segments = Sets.newLinkedHashSet();

    for (TableDataSource table : dataSources) {
      TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(table);

      for (Interval interval : intervals) {
        List<TimelineObjectHolder<String, ServerSelector>> serversLookup = timeline.lookup(interval);

        for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
          for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
            ServerSelector server = chunk.getObject();
            final SegmentDescriptor segment = new SegmentDescriptor(
                holder.getInterval(),
                holder.getVersion(),
                chunk.getChunkNumber()
            );
            segments.add(new ServerToSegment(server, segment));
          }
        }
      }
    }

    return segments;
  }

  @Override
  public List<String> getNames()
  {
    return null;
  }
}
class ServerToSegment extends Pair<ServerSelector, SegmentDescriptor>
{
  public ServerToSegment(ServerSelector server, SegmentDescriptor segment)
  {
    super(server, segment);
  }

  ServerSelector getServer()
  {
    return lhs;
  }

  SegmentDescriptor getSegmentDescriptor()
  {
    return rhs;
  }
}

