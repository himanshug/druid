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

package io.druid.atomix;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.zk.Node;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public class GroupJoiner
{
  private static final EmittingLogger log = new EmittingLogger(GroupJoiner.class);

  private final CopycatClientProvider atomix;
  private final Node node;
  private final ObjectMapper mapper;

  @Inject
  public GroupJoiner(CopycatClientProvider atomix, Node node, ObjectMapper mapper)
  {
    this.atomix = atomix;
    this.node = node;
    this.mapper = mapper;
  }

  @LifecycleStart
  public void start()
  {
    try {
      log.info("Starting.");

      joinGroup().join();
      atomix.onStateChange(this::onStateChange);

      log.info("Started.");
    }
    catch (Exception ex) {
      throw new ISE(ex, "failed to start");
    }
  }

  @LifecycleStop
  public void stop()
  {
  }

  private void onStateChange(CopycatClientProvider.State state) {
    if (state == CopycatClientProvider.State.CONNECTED) {
      joinGroup();
    }
  }

  private CompletableFuture<Node> joinGroup()
  {
    log.info("Submitting JoinCommand for [%s].", node);
    return atomix.submit(new JoinCommand(node))
                 .whenComplete(
                     (v, e) -> {
                       if (v != null) {
                         log.info("JoinCommand succeeded for member [%s].", node);
                       } else {
                         log.makeAlert(e, "Member [%s] failed to join group.", node).emit();
                       }
                     }
                 );
  }
}
