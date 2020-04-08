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

package org.apache.druid.security.basic.authorization;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.server.security.AuthenticationResult;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@JsonTypeName("context")
public class ContextKeyRoleProvider implements RoleProvider
{
  private static final String CTX_ROLES_KEY = "__druid_basic_security_roles";

  private final BasicAuthorizerCacheManager cacheManager;

  @JsonCreator
  public ContextKeyRoleProvider(
      @JacksonInject BasicAuthorizerCacheManager cacheManager
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  public Set<String> getRoles(
      String authorizerPrefix,
      AuthenticationResult authenticationResult
  )
  {
    return (Set<String>) authenticationResult.getContext().getOrDefault(CTX_ROLES_KEY, Collections.EMPTY_SET);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix)
  {
    return cacheManager.getRoleMap(authorizerPrefix);
  }
}
