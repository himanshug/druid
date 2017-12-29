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
import io.druid.server.coordination.zk.Node;
import io.druid.server.coordination.zk.NodeDiscovery;
import io.druid.server.coordination.zk.NodeGroupChangeListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 */
public class GroupStateMachineClient implements NodeDiscovery
{
  private static final EmittingLogger log = new EmittingLogger(GroupStateMachineClient.class);

  private final CopycatClientProvider atomix;

  private final Map<NodeGroupChangeListener, Listener> listeners = new ConcurrentHashMap<>();

  private Map<String, Node> members = new HashMap<>();


  @Inject
  public GroupStateMachineClient(CopycatClientProvider atomix)
  {
    this.atomix = atomix;
  }

  @LifecycleStart
  void start()
  {
    log.info("Starting.");

    atomix.onStateChange(this::onStateChange);
    atomix.onEvent(GroupStateMachine.JOIN, this::onJoinEvent);
    atomix.onEvent(GroupStateMachine.LEAVE, this::onLeaveEvent);
    atomix.submit(new ListenCommand()).thenAccept(
        status -> {
          for (Node member : status.getMembers()) {
            addMember(member);
          }
        }
    ).join();

    log.info("Started.");
  }

  @LifecycleStop
  void stop()
  {

  }

  private synchronized void onJoinEvent(Node member)
  {
    log.info("Join Event [%s].", member);
    addMember(member);
  }

  private synchronized void onLeaveEvent(String id)
  {
    log.info("Leave Event [%s].", id);
    removeMember(id);
  }

  private synchronized void onStateChange(CopycatClientProvider.State state) {
    if (state == CopycatClientProvider.State.CONNECTED) {
      recover();
    }
  }

  private synchronized void addMember(Node member)
  {
    if (!members.containsKey(member.getName())) {
      log.debug("Member [%s] added.", member);
      members.put(member.getName(), member);
      for (Listener listener : listeners.values()) {
        listener.nodeAdded(member);
      }
    }
  }

  private synchronized void removeMember(String id)
  {
    Node member = members.remove(id);
    if (member != null) {
      log.debug("Member [%s] removed.", member);
      for (Listener listener : listeners.values()) {
        listener.nodeRemoved(member);
      }
    }
  }

  private void recover()
  {
    log.debug("Starting Recovery.");
    atomix.submit(new ListenCommand()).thenApply(
        status -> {
          synchronized (GroupStateMachineClient.this) {
            Map<String, Node> newMembers = new HashMap<>();
            for (Node member : status.getMembers()) {
              newMembers.put(member.getName(), member);
              if (!members.containsKey(member.getName())) {
                addMember(member);
              }
            }

            for (Node m : members.values()) {
              if (!newMembers.containsKey(m.getName())) {
                removeMember(m.getName());
              }
            }

            log.info("Swapping members list post recovery. New members [%s].", newMembers);
            members = newMembers;
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
  public void registerGroupChangeListener(NodeGroupChangeListener listener)
  {
    Listener wrapper = new Listener(listener);
    for (Node member : members.values()) {
      wrapper.nodeAdded(member);
    }
    listeners.put(listener, wrapper);
  }

  @Override
  public void unregisterGroupChangeListener(NodeGroupChangeListener listener)
  {
    Listener wrapper = listeners.remove(listener);
    if (wrapper != null) {
      wrapper.close();
    }
  }

  private static class Listener
  {
    private final NodeGroupChangeListener delegate;
    private final ExecutorService executorService;

    private volatile boolean closed = false;

    public Listener(NodeGroupChangeListener delegate)
    {
      this.delegate = delegate;
      this.executorService = Executors.newSingleThreadExecutor(); //TODO: FIFO and Naming
    }

    synchronized void nodeAdded(final Node node)
    {
      execute(node, delegate::serverAdded);
    }

    synchronized void nodeRemoved(final Node node)
    {
      execute(node, delegate::serverRemoved);
    }

    synchronized void execute(Node node, Consumer<Node> consumer)
    {
      if (closed) {
        return;
      }

      executorService.execute(() -> {
        try {
          consumer.accept(node);
        }
        catch (Exception ex) {
          log.error(ex, "Exception executing listener.");
        }
      });
    }

    synchronized void close()
    {
      closed = true;
      executorService.shutdownNow();
    }
  }
}


