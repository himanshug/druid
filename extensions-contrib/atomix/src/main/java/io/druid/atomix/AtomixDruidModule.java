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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.server.coordination.zk.CoordinatorRole;
import io.druid.server.coordination.zk.LeaderDiscovery;
import io.druid.server.coordination.zk.Node;
import io.druid.server.coordination.zk.NodeDiscovery;
import io.druid.server.coordination.zk.Role;

import java.util.List;

/**
 * mvn dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target
 */
public class AtomixDruidModule implements DruidModule
{
  private static String TYPE = "copycat";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.atomix", AtomixConfig.class);

    binder.bind(CopycatClientProvider.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, CopycatClientProvider.class);

    binder.bind(GroupJoiner.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, GroupJoiner.class);

    //can we get instance of
    //This should only be needed on coordinator/broker ?
    binder.bind(GroupStateMachineClient.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, GroupStateMachineClient.class);
    binder.bind(CopycatLeaderDiscovery.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, CopycatLeaderDiscovery.class);

    //bind NodeDiscovery and LeadershipDiscovery implementations
    PolyBind.optionBinder(binder, Key.get(NodeDiscovery.class))
            .addBinding(TYPE)
            .to(GroupStateMachineClient.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(LeaderDiscovery.class))
            .addBinding(TYPE)
            .to(CopycatLeaderDiscovery.class)
            .in(LazySingleton.class);
  }

  @Provides
  public CopycatServerProvider getCopycatServerProvider(
      Node node,
      AtomixConfig config,
      ObjectMapper mapper
  )
  {
    if (isCoordinator(node)) {
      System.err.println("creating real server provider for node " + node);
      return new CopycatServerProvider(config, mapper, false);
    } else {
      System.err.println("creating noope server provider for node " + node);
      return new CopycatServerProvider(config, mapper, true);
    }
  }

  private boolean isCoordinator(Node node)
  {
    for (Role role : node.getRoles()) {
      if (role instanceof CoordinatorRole) {
        return true;
      }
    }

    return false;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }
}
