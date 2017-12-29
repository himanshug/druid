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
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.druid.atomix.discovery.ServicesStateMachine;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.time.Duration;

/**
 *  * TODO or QQ
 * - on start, bootstrap(cluster).join what happens if nodes are started one by one?
 * - on stop, should leave and shutDown or just shutDown? it should do the "leave" too and on next start it will join
 *  the cluster again via the bootstrap process. So planned shutdown will make reduce the number of nodes in raft cluster.
 *  However, if a machine went down and can't come up, then we need to support a "REMOVE NODE" http call to remove that
 *  particular node from the RAFT cluster.
 *  And when a node comes up new it will still work with bootstrap servers even if they have left the quorum.
 *
 * - Use a directory for storing the LOG, instructions on what happens when LOG on one or more hosts is corrupted. If
 *     we use MEMORY storage, then is there an upper bound on memory taken?
 *
 * on server crash session events are recovered by replaying the log, will things work with MEMORY storage?
 *  probably yes, because other server wouldn't have discraded the log just yet and it will be copied to
 *  recovered server.
 *
 * with MEMORY store, "term" and "votedFor" is in MEMORY of a node, if node is restarted then it might vote for another
 * candidate in the same term, that will break RAFT's protocol? no
 *
 * Do you flush disk when DISK type storage is written? How is corruption handled, Are there checksums?
 *
 * Is there any reason of using on-heap memory, it seems you pretty much know when to allocate/deallocate and GC scans
 * that memory for no reason. should I introduce a OFFHEAP_MEMORY storage level?
 *
 * If I can't use MEMORY storage then when is it ever useful?
 *
 *
 * C is connected to S and C is the leader at this time, there is a network partition.. S's state machine is not updated
 * anymore C will continue to think that it is the leader but rest of raft cluster will find another leader?
 * Ideally, if C goes to SUSPENDED state it should give up leadership if it had it.
 *
 * Instead just have term events, and elect leader on MembershipGroup, on reaching SUSPENDED state, resign the leader
 * when we connect again, we'll sync again and leader will appear again.
 *
 * if using DISK storage, then a coordinator needs to be formally removed from cluster by doing a remove coordinator call.
 * OTOH, using MEMORY we run the risk of causing OOMs.
 */
public class CopycatServerProvider
{
  private static final Logger log = new Logger(CopycatServerProvider.class);

  private CopycatServer server;
  private final AtomixConfig config;
  private final ObjectMapper mapper;
  final boolean noop;

  public CopycatServerProvider(AtomixConfig config, ObjectMapper mapper, boolean noop)
  {
//    this.noop = noop;
    this.noop = config.isServer;
    this.config = config;
    this.mapper = mapper;

  }

  public void start()
  {
    if (noop) {
      return;
    }

    if (!config.isEnabled()) {
      return;
    }

    try {
      log.info("Starting.");

      server = CopycatServer.builder(config.getNodeAddress())
                            .withStateMachine(ServicesStateMachine::new)
                            .withTransport(new NettyTransport())
                            .withStorage(
                                Storage.builder()
                                       .withDirectory(config.workDir)
                                       .withStorageLevel(StorageLevel.MEMORY)
                                       .withMaxSegmentSize(1024 * 1024 * 32)
                                       .withMinorCompactionInterval(
                                           Duration.ofMinutes(
                                               1
                                           )
                                       )
                                       .withMajorCompactionInterval(Duration.ofMinutes(15))
                                       .build()
                            )
                            .withHeartbeatInterval(Duration.ofMillis(config.getHeartbeatInterval()))
                            .withElectionTimeout(Duration.ofMillis(config.getElectionTimeout()))
                            .build();

      CopycatClientProvider.updateSerializer(server.serializer(), mapper);

      server.bootstrap(config.getBootstrapServers()).join();

      log.info("Started.");
    } catch (Exception ex) {
      throw new ISE(ex, "failed to start");
    }
  }

  public void stop()
  {
    if (noop) {
      return;
    }

    if (server != null) {
      try {
        log.info("Stopping.");
        server.shutdown().join();
        log.info("Stopped.");
      } catch (Exception ex) {
        log.error(ex, "Failed to stop.");
      }
    }
  }

  public Address getServerAddress()
  {
    if (noop) {
      return null;
    } else {
      return config.getNodeAddress();
    }
  }
}
