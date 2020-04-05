/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.pac4j.core.config.Config;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("pac4j")
public class Pac4jAuthenticator implements Authenticator
{
  private final String name;
  private final String authorizerName;
  private final Supplier<Config> pac4jConfigSupplier;
  private final Pac4jCommonConfig pac4jCommonConfig;

  @JsonCreator
  public Pac4jAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JacksonInject Pac4jCommonConfig pac4jCommonConfig,
      @JacksonInject Pac4jClientFactory pac4jClientFactory
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.pac4jCommonConfig = pac4jCommonConfig;
    this.pac4jConfigSupplier = Suppliers.memoize(
        () -> new Config(Pac4jCallbackResource.SELF_URL, pac4jClientFactory.getClient())
    );
  }

  @Override
  public Filter getFilter()
  {
    return new Pac4jFilter(
        name,
        authorizerName,
        pac4jConfigSupplier.get(),
        pac4jCommonConfig.getCookiePassphrase().getPassword()
    );
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return null;
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    return null;
  }


  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }
}
