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

import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.zk.LeaderDiscovery;
import io.druid.server.coordination.zk.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class CopycatLeaderDiscovery implements LeaderDiscovery
{
  private static final EmittingLogger log = new EmittingLogger(CopycatLeaderDiscovery.class);

  private final CopycatClientProvider atomix;
  private final Node thisNode;

  private final List<LeaderListener> coordinatorLeadershipListeners = new ArrayList<>();
  private Node coordinatorLeader;


  @Inject
  public CopycatLeaderDiscovery(Node thisNode, CopycatClientProvider atomix)
  {
    this.thisNode = thisNode;
    this.atomix = atomix;
  }

  @LifecycleStart
  void start()
  {
    log.info("Starting.");

    atomix.onStateChange(this::onStateChange);
    atomix.onEvent(GroupStateMachine.COORDINATOR_LEADER, this::onCoordinatorLeader);
    atomix.submit(new ListenCommand()).thenAccept(
        status -> {
          onCoordinatorLeader(status.getCoordinatorLeader());
        }
    ).join();

    log.info("Started.");
  }

  @LifecycleStop
  void stop()
  {

  }

  private synchronized void onCoordinatorLeader(Node leaderInfo)
  {
    Node prevLeader = coordinatorLeader;
    coordinatorLeader = leaderInfo;

    if (thisNode.getName().equals(coordinatorLeader != null ? coordinatorLeader.getName() : null)) {
      if (prevLeader == null || !thisNode.getName().equals(prevLeader != null ? prevLeader.getName() : null)) {
        //you were not a leader, take leadership
        for (LeaderListener listener : coordinatorLeadershipListeners) {
          listener.takeLeadership();
        }
      }
    } else {
      if (prevLeader != null && thisNode.getName().equals(prevLeader != null ? prevLeader.getName() : null)) {
        //you were a leader, leave leadership
        for (LeaderListener listener : coordinatorLeadershipListeners) {
          listener.leaveLeadership();
        }
      }
    }
  }

  private synchronized void onStateChange(CopycatClientProvider.State state) {
    if (state == CopycatClientProvider.State.CONNECTED) {
      recover();
    } else {
      if (coordinatorLeader != null) {
        onCoordinatorLeader(null);
      }
    }
  }

  private void recover()
  {
    log.debug("Starting Recovery.");
    atomix.submit(new ListenCommand()).thenApply(
        status -> {
          synchronized (CopycatLeaderDiscovery.this) {
            onCoordinatorLeader(status.getCoordinatorLeader());
          }
          return this;
        }
    ).whenComplete(
        (v, e) -> {
          if (v != null) {
            log.debug("Finished Recovery Successfully.");
          } else {
            //TODO: stop being leader and is it possible to retry? this error can happen due to
            //multiple unknown reasons, we should continue to retry instead.
            log.makeAlert(e, "Group Recovery failed.");
          }
        }
    );
  }

  @Override
  public synchronized void registerCoordinatorLeadershipListener(LeaderDiscovery.Listener listener)
  {
    //TODO: check that it is started

    LeaderListener leaderListener = new LeaderListener(listener);
    coordinatorLeadershipListeners.add(leaderListener);

    if (coordinatorLeader != null && thisNode.getName().equals(coordinatorLeader.getName())) {
      leaderListener.takeLeadership();
    }
  }

  @Override
  public Node getCoordinatorLeader()
  {
    return coordinatorLeader;
  }
}

class LeaderListener
{
  private final LeaderDiscovery.Listener delegate;
  private final ExecutorService executorService;

  public LeaderListener(LeaderDiscovery.Listener delegate)
  {
    this.delegate = delegate;
    this.executorService = Executors.newSingleThreadExecutor(); //TODO: FIFO and Naming
  }

  synchronized void takeLeadership()
  {
    executorService.execute(
        new Runnable()
        {
          @Override
          public void run()
          {
            delegate.takeLeadership();
          }
        }
    );
  }

  synchronized void leaveLeadership()
  {
    executorService.execute(
        new Runnable()
        {
          @Override
          public void run()
          {
            delegate.leaveLeadership();
          }
        }
    );
  }
}
