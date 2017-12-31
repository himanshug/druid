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
public class ServicesStateMachine // extends StateMachine implements SessionListener
{ /*
  private static final Logger log = new Logger(ServicesStateMachine.class);

  static final String JOIN = "join";
  static final String LEAVE = "leave";
  static final String COORDINATOR_LEADER = "coordinatorLeader";

  //TODO: on removal, remove the empty lists.

  //service -> list of watcher sessions
  private final Map<String, Set<ServerSession>> listeners = new HashMap<>();
  private final Map<ServerSession, Set<String>> listenersBySession = new HashMap<>();


  private final Map<ServerSession, Set<MemberHolder>> members = new HashMap<>();
  private final Map<String, MemberHolder> membersById = new HashMap<>();
  private final Map<String, Set<MemberHolder>> membersByService = new HashMap<>();

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(JoinCommand.class, this::join);
    executor.register(ListenCommand.class, this::listen);
  }

  public static String joinSessionEventNameFor(String serviceName)
  {
    return JOIN+serviceName;
  }

  public static String leaveSessionEventNameFor(String serviceName)
  {
    return LEAVE+serviceName;
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

  public DruidNode join(Commit<JoinCommand> commit) {
    try {
      MemberHolder member = membersById.get(commit.command().getNode().getHostAndPort());

      if (member == null) {
        member = new MemberHolder(commit);

        membersById.put(member.member.getHostAndPort(), member);

        Set<MemberHolder> sessionMember = members.get(commit.session());
        if (sessionMember == null) {
          sessionMember = new HashSet<>();
          members.put(commit.session(), sessionMember);
        }
        sessionMember.add(member);

        Set<MemberHolder> serviceMember = membersByService.get(member.member.getServiceName());
        if (serviceMember == null) {
          serviceMember = new HashSet<>();
          membersByService.put(member.member.getServiceName(), serviceMember);
        }
        serviceMember.add(member);

        for (ServerSession s : listeners.getOrDefault(member.member.getServiceName(), ImmutableSet.of())) {
          s.publish(joinSessionEventNameFor(member.member.getServiceName()), member.getMemberInfo());
        }

        log.debug("Member [%s] added.", member.getMemberInfo());
      } else {
        if (member.commit.session() == commit.session()) {
          log.warn("Received member add [%s] but it already exists.");
        } else {
          log.warn("Received member add [%s] but it already exists from another session.");
        }
      }

      return member.getMemberInfo();
    } catch (Exception ex) {
      commit.close();
      log.error(ex,"exception joining member");
      throw ex;
    }
  }

  public List<DruidNode> listen(Commit<ListenCommand> commit) {
    log.debug("Received Listen Command Request from session id [%s].", commit.session().id());

    String service = commit.command().getService();

    Set<ServerSession> sessions = listeners.get(service);
    if (sessions == null) {
      sessions = new HashSet<>();
      listeners.put(service, sessions);
    }
    sessions.add(commit.session());

    Set<String> services = listenersBySession.get(commit.session());
    if (services == null) {
      services = new HashSet<>();
      listenersBySession.put(commit.session(), services);
    }
    services.add(service);

    Set<MemberHolder> members = membersByService.get(commit.command().getService());
    if (members == null) {
      return ImmutableList.of();
    } else {
      List<DruidNode> result = new ArrayList<>();
      for (MemberHolder m : members) {
        result.add(m.member);
      }
      return result;
    }
  }

  private void remove(ServerSession session)
  {
    Set<String> services = listenersBySession.remove(session);
    if (services != null) {
      for (String s : services) {
        listeners.get(s).remove(session);
      }
    }

    Set<MemberHolder> memberList = members.remove(session);
    if (memberList == null) {
      return;
    }

    for (MemberHolder m : memberList) {
      m.commit.close();
      //TODO: check and log error if that member is not found in following places
      membersById.remove(m.member.getHostAndPort());
      membersByService.get(m.member.getServiceName()).remove(m);


      for (ServerSession s : listeners.getOrDefault(m.member.getServiceName(), ImmutableSet.of())) {
        s.publish(leaveSessionEventNameFor(m.member.getServiceName()), m.member.getHostAndPort());
      }

      log.info("Member [%s] removed.", m.getMemberInfo());
    }
  } */
}

class MemberHolder
{
  /*
  Commit<JoinCommand> commit;
  DruidNode member;

  MemberHolder(Commit<JoinCommand> commit)
  {
    this.commit = commit;
    this.member = commit.command().getNode();
  }

  DruidNode getMemberInfo()
  {
    return member;
  }
  */
}

