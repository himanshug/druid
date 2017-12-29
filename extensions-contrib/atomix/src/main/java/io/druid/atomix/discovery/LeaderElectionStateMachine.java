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

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;

/**
 */
public class LeaderElectionStateMachine extends StateMachine implements SessionListener
{
  private static final Logger log = new Logger(LeaderElectionStateMachine.class);

  static final String JOIN = "join";
  static final String LEAVE = "leave";
  static final String COORDINATOR_LEADER = "coordinatorLeader";

  //maintain following
  // list of candidates


  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(JoinCommand.class, this::join); //TODO: has to be a different name
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

  //This returns the "leader"
  public DruidNode join(Commit<JoinCommand> commit) {
    try {
      //put this guy in candidates , watchers  and do leader election if needed
      return null;

    } catch (Exception ex) {
      commit.close();
      log.error(ex,"exception joining member");
      throw ex;
    }
  }

  private void remove(ServerSession session)
  {
    //remove candidate from this session and do leader election if necessary
  }
}
