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
public class CopycatDruidNodeDiscoveryProvider // implements DruidNodeDiscoveryProvider
{
  /*
  private static final Logger log = new Logger(CopycatDruidNodeDiscoveryProvider.class);

  private final CopycatClientProvider atomix;
  private final Map<String, DruidNodeDiscoveryImpl> impls = new HashMap<>();

  private volatile boolean stopped = false;

  @Inject
  public CopycatDruidNodeDiscoveryProvider(
      CopycatClientProvider atomix
  )
  {
    this.atomix = atomix;
  }

  @Override
  public synchronized DruidNodeDiscovery getForService(String serviceName)
  {
    if (stopped) {
      throw new ISE("Provider has been stopped.");
    }

    DruidNodeDiscoveryImpl impl = impls.get(serviceName);
    if (impl == null) {
      log.debug("Creating/Starting DruidServiceDiscovery for service [%s].", serviceName);
      impl = new DruidNodeDiscoveryImpl(atomix, serviceName);
      impl.start();
      log.debug("Created/Started DruidServiceDiscovery for service [%s].", serviceName);
    }

    return impl;
  }

  @LifecycleStart
  public void start()
  {
    log.info("started");
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (stopped) {
      return;
    }
    stopped = true;

    log.info("stopping");

    for (DruidNodeDiscoveryImpl impl : impls.values()) {
      impl.stop();
    }

    log.info("stopped");
  }

  private static class DruidNodeDiscoveryImpl implements DruidNodeDiscovery
  {
    private static final Logger log = new Logger(DruidNodeDiscoveryImpl.class);

    private final CopycatClientProvider atomix;

    private String serviceName;

    private Map<String, DruidNode> members = new HashMap<>();

    private List<DruidNodeDiscovery.Listener> listeners = new ArrayList<>();

    private ExecutorService executorService = Executors.newSingleThreadExecutor(); //TODO: naming, daemon , FIFO

    public DruidNodeDiscoveryImpl(CopycatClientProvider atomix, String serviceName)
    {
      this.atomix = atomix;
      this.serviceName = serviceName;
    }

    void start()
    {
      log.info("Starting.");

      atomix.onStateChange(this::onStateChange);
      atomix.onEvent(ServicesStateMachine.joinSessionEventNameFor(serviceName), this::onJoinEvent);
      atomix.onEvent(ServicesStateMachine.leaveSessionEventNameFor(serviceName), this::onLeaveEvent);
      atomix.submit(new ListenCommand(serviceName)).thenAccept(
          status -> {
            for (DruidNode member : status) {
              addMember(member);
            }
          }
      ).join();

      log.info("Started.");
    }

    void stop()
    {

    }

    private synchronized void onJoinEvent(DruidNode member)
    {
      log.info("Join Event [%s].", member);
      addMember(member);
    }

    private synchronized void onLeaveEvent(String id)
    {
      log.info("Leave Event [%s].", id);
      removeMember(id);
    }

    private synchronized void onStateChange(CopycatClientProvider.State state)
    {
      if (state == CopycatClientProvider.State.CONNECTED) {
        recover();
      }
    }

    private synchronized void addMember(DruidNode member)
    {
      if (!members.containsKey(member.getHostAndPort())) {
        log.debug("Member [%s] added.", member);
        members.put(member.getHostAndPort(), member);
        for (DruidNodeDiscovery.Listener listener : listeners) {
          executorService.submit(() -> {
            try {
              listener.nodeAdded(member);
            }
            catch (Exception ex) {
              log.error(ex, "nodeAdded() call failed on listener type [%s].", listener.getClass().getName());
            }
          });
        }
      }
    }

    private synchronized void removeMember(String id)
    {
      DruidNode member = members.remove(id);
      if (member != null) {
        log.debug("Member [%s] removed.", member);
        for (DruidNodeDiscovery.Listener listener : listeners) {
          executorService.submit(() -> {
            try {
              listener.nodeRemoved(member);
            }
            catch (Exception ex) {
              log.error(ex, "nodeAdded() call failed on listener type [%s].", listener.getClass().getName());
            }
          });
        }
      }
    }

    private void recover()
    {
      log.debug("Starting Recovery.");
      atomix.submit(new ListenCommand(serviceName)).thenApply(
          status -> {
            synchronized (DruidNodeDiscoveryImpl.this) {
              Map<String, DruidNode> newMembers = new HashMap<>();
              for (DruidNode member : status) {
                newMembers.put(member.getHostAndPort(), member);
                if (!members.containsKey(member.getHostAndPort())) {
                  addMember(member);
                }
              }

              for (DruidNode m : members.values()) {
                if (!newMembers.containsKey(m.getHostAndPort())) {
                  removeMember(m.getHostAndPort());
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
              log.error(e, "Group Recovery failed.");
            }
          }
      );
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
      listeners.add(listener);

      for (DruidNode node : members.values()) {
        executorService.submit(() -> {
          try {
            listener.nodeAdded(node);
          }
          catch (Exception ex) {
            log.error(ex, "nodeAdded() call failed on listener type [%s].", listener.getClass().getName());
          }
        });
      }
    }
  } */
}
