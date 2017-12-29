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

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.session.Session;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordination.zk.Node;
import io.druid.server.coordination.zk.NodeDiscovery;
import io.druid.server.coordination.zk.Role;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupStateMachine extends StateMachine implements SessionListener
{
  private static final Logger log = new Logger(GroupStateMachine.class);

  static final String JOIN = "join";
  static final String LEAVE = "leave";
  static final String COORDINATOR_LEADER = "coordinatorLeader";

  private final Set<ServerSession> listeners = new HashSet<>();
  private final Map<ServerSession, MemberHolder> members = new HashMap<>();

  private final List<MemberHolder> coordinatorCandidates = new ArrayList<>();
  private volatile MemberHolder coordinatorLeader = null;

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(JoinCommand.class, this::join);
    executor.register(ListenCommand.class, this::listen);
  }

  @Override
  public void register(ServerSession serverSession)
  {
    //do nothing
  }

  @Override
  public void unregister(ServerSession session) {
    //log unregister
    remove(session);
  }

  @Override
  public void expire(ServerSession session) {
    //log expiry
    remove(session);
  }

  @Override
  public void close(ServerSession serverSession)
  {
    //do nothing, unregister/expire would be called before this
  }

  public Node join(Commit<JoinCommand> commit) {
    try {
      MemberHolder member = members.get(commit.session());

      if (member == null) {
        member = new MemberHolder(commit, commit.session());
        members.put(commit.session(), member);

        for (ServerSession s : listeners) {
          s.publish(JOIN, member.getMemberInfo());
        }

        if (isCoordinatorNode(member.getMemberInfo())) {
          coordinatorCandidates.add(member);
          if (coordinatorLeader == null) {
            coordinatorLeader = electLeader(COORDINATOR_LEADER, coordinatorCandidates);
          }
        }

        log.debug("Member [%s] added.", member.getMemberInfo());
      } else {
        //check and log error if its different node info, also this commit
        //needs to be closed.
        log.warn("Received member add [%s] but it already exists.");
      }

      return member.getMemberInfo();
    } catch (Exception ex) {
      commit.close();
      log.error(ex,"exception joining member");
      throw ex;
    }
  }

  public GroupInfo listen(Commit<ListenCommand> commit) {
    log.debug("Received Listen Command Request from session id [%s].", commit.session().id());

    listeners.add(commit.session()); //may be add a separate listen call or rename it to listen call.
    List<Node> memberInfos = new ArrayList<>();
    for (MemberHolder member : members.values()) {
      memberInfos.add(member.getMemberInfo());
    }
    return new GroupInfo(memberInfos, coordinatorLeader != null ? coordinatorLeader.getMemberInfo() : null);
  }

  private void remove(Session session)
  {
    if (coordinatorLeader == null) {
      //log alert
    }

    listeners.remove(session);

    MemberHolder member = members.remove(session);
    if (member == null) {
      return;
    }
    member.commit.close();


    if (isCoordinatorNode(member.getMemberInfo())) {
      coordinatorCandidates.remove(member);

      if (coordinatorLeader != null && coordinatorLeader.equals(member)) {
        coordinatorLeader = electLeader(COORDINATOR_LEADER, coordinatorCandidates);
      }
    }

    for (ServerSession s : listeners) {
      s.publish(LEAVE, member.getMemberInfo().getName());
    }

    log.info("Member [%s] removed.", member.getMemberInfo());
  }

  private MemberHolder electLeader(String eventName, List<MemberHolder> candidates)
  {
    MemberHolder leader = null;
    if (candidates.size() > 0) {
      Collections.sort(
          candidates,
          (m1, m2) -> m1.getMemberInfo().getName().compareTo(m2.getMemberInfo().getName())
      );
      leader = candidates.get(0);
    }

    for (ServerSession s : listeners) {
      s.publish(eventName, leader);
    }

    return leader;
  }

  private boolean isCoordinatorNode(Node node)
  {
    for (Role role : node.getRoles()) {
      if (NodeDiscovery.COORDINATOR_ID.equals(role.getId())) {
        return true;
      }
    }
    return false;
  }
}

class MemberHolder
{
  Commit<JoinCommand> commit;
  ServerSession session;
  Node member;

  MemberHolder(Commit<JoinCommand> commit, ServerSession session)
  {
    this.commit = commit;
    this.member = commit.operation().getMember();
    this.session = session;
  }

  Node getMemberInfo()
  {
    return member;
  }
}

