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

package io.druid.atomix.raft;

import com.metamx.emitter.EmittingLogger;
import io.druid.atomix.discovery.CopycatDruidServiceAnnouncer;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeAnnouncer;
import io.druid.guice.annotations.Json;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Inject;

/**
 */
public class RaftDruidNodeAnnouncer implements DruidNodeAnnouncer
{
  private static final EmittingLogger log = new EmittingLogger(CopycatDruidServiceAnnouncer.class);

  private final RaftClientProvider atomix;
  private final ObjectMapper mapper;

  @Inject
  public RaftDruidNodeAnnouncer(RaftClientProvider atomix, @Json ObjectMapper mapper)
  {
    this.atomix = atomix;
    this.mapper = mapper;
  }

  //TODO: reannounce node if we got disconnected and reconnected etc.

  @Override
  public void announce(DiscoveryDruidNode discoveryDruidNode)
  {

  }

  @Override
  public void unannounce(DiscoveryDruidNode discoveryDruidNode)
  {

  }
}
