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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.atomix.catalyst.transport.Address;

import java.util.List;

/**
 */
public class AtomixConfig
{
  //note that ports here need to be netty transport ports and not http ports

  @JsonProperty
  boolean isServer = false;

  @JsonProperty
  boolean enabled = false;

  @JsonProperty
  List<String> bootstrapServers;

  @JsonProperty
  String nodeAddress;

  @JsonProperty
  String workDir = System.getProperty("java.io.tmpdir") + "/druid-copycat/";

  //TODO
  //sessionTimeout
  //heartbeat frequency

  public AtomixConfig()
  {
    //used by Jackson
  }

  @VisibleForTesting
  AtomixConfig(
      String nodeAddress,
      List<String> bootstrapServers
  )
  {
    this.enabled = true;
    this.nodeAddress = nodeAddress;
    this.bootstrapServers = bootstrapServers;
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public List<Address> getBootstrapServers()
  {
    return Lists.transform(
        bootstrapServers,
        new Function<String, Address>()
        {
          @Override
          public Address apply(String input)
          {
            return new Address(input);
          }
        }
    );
  }

  public Address getNodeAddress()
  {
    return new Address(nodeAddress);
  }

  public long getSessionTimeout()
  {
    return 700;
  }

  public long getElectionTimeout()
  {
    return 700;
  }

  public long getHeartbeatInterval()
  {
    return 300;
  }

  @Override
  public String toString()
  {
    return "AtomixConfig{" +
           "enabled=" + enabled +
           ", bootstrapServers=" + bootstrapServers +
           ", nodeAddress=" + nodeAddress +
           '}';
  }
}
