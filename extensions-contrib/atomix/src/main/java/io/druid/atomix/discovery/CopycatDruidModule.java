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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.druid.atomix.AtomixConfig;
import io.druid.atomix.CopycatClientProvider;
import io.druid.atomix.CopycatServerProvider;
import io.druid.discovery.DruidServiceAnnouncer;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 */
public class CopycatDruidModule implements DruidModule
{
  private static String TYPE = "copycat";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.atomix", AtomixConfig.class);

    binder.bind(CopycatClientProvider.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, CopycatClientProvider.class);

    PolyBind.optionBinder(binder, Key.get(DruidServiceAnnouncer.class))
            .addBinding(TYPE)
            .to(CopycatDruidServiceAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeDiscoveryProvider.class))
            .addBinding(TYPE)
            .to(CopycatDruidNodeDiscoveryProvider.class)
            .in(LazySingleton.class);
  }

  //TODO how to discover if it is coordinator and start server based on that only.
  @Provides
  public CopycatServerProvider getCopycatServerProvider(
      AtomixConfig config,
      ObjectMapper mapper
  )
  {
    return new CopycatServerProvider(config, mapper, false);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }
}
