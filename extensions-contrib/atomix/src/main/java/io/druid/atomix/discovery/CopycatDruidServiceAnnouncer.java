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

package io.druid.atomix.discovery;

/**
 */
public class CopycatDruidServiceAnnouncer // implements DruidServiceAnnouncer
{/*
  private static final EmittingLogger log = new EmittingLogger(CopycatDruidServiceAnnouncer.class);

  private final CopycatClientProvider atomix;
  private final List<DruidNode> nodes = new ArrayList<>();
  private final ObjectMapper mapper;

  @Inject
  public CopycatDruidServiceAnnouncer(CopycatClientProvider atomix, ObjectMapper mapper)
  {
    this.atomix = atomix;
    this.mapper = mapper;
  }

  @LifecycleStart
  public void start()
  {
    log.info("Starting.");
    atomix.onStateChange(this::onStateChange);
    log.info("Started.");
  }

  @LifecycleStop
  public void stop()
  {
    //unannounce not necessary as copycat session close will itself unannounce everything.
    log.info("Stopped.");
  }

  private void onStateChange(CopycatClientProvider.State state) {
    if (state == CopycatClientProvider.State.CONNECTED) {
      announceAll();
    }
  }

  private CompletableFuture<DruidNode> announceToCopycat(DruidNode node)
  {
    return atomix.submit(new JoinCommand(node))
                 .whenComplete(
                     (v, e) -> {
                       if (v != null) {
                         log.info("JoinCommand succeeded for member [%s].", node);
                       } else {
                         log.error(e, "Member [%s] failed to join group.", node);
                       }
                     }
                 );
  }

  private synchronized void announceAll()
  {
    for (DruidNode node : nodes) {
      announceToCopycat(node);
    }
  }

  @Override
  public synchronized void announce(DruidNode node)
  {
    announceToCopycat(node).join();
    nodes.add(node);
  }

  @Override
  public synchronized void unannounce(DruidNode node)
  {
    //TODO
  } */
}
