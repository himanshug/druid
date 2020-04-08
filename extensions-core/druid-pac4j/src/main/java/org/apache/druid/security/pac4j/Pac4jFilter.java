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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.J2EContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.engine.SecurityLogic;
import org.pac4j.core.http.adapter.J2ENopHttpActionAdapter;
import org.pac4j.core.profile.CommonProfile;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class Pac4jFilter implements Filter
{
  private static final Logger LOGGER = new Logger(Pac4jFilter.class);
  
  private final Config pac4jConfig;
  private final SecurityLogic<Object, J2EContext> securityLogic;
  private final CallbackLogic<Object, J2EContext> callbackLogic;
  private final SessionStore<J2EContext> sessionStore;

  private final String name;
  private final String authorizerName;

  public Pac4jFilter(String name, String authorizerName, Config pac4jConfig, String cookiePassphrase)
  {
    this.pac4jConfig = pac4jConfig;
    this.securityLogic = new DefaultSecurityLogic<>();
    this.callbackLogic = new DefaultCallbackLogic<>();

    this.name = name;
    this.authorizerName = authorizerName;

    this.sessionStore = new Pac4jSessionStore<>(cookiePassphrase);
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
  }


  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException
  {
    // If there's already an auth result, then we have authenticated already, skip this or else caller
    // could get HTTP redirect even if one of the druid authenticators in chain has successfully authenticated.
    if (servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) != null) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
    J2EContext context = new J2EContext(httpServletRequest, httpServletResponse, sessionStore);

    if (Pac4jCallbackResource.SELF_URL.equals(httpServletRequest.getRequestURI())) {
      callbackLogic.perform(
          context,
          pac4jConfig,
          J2ENopHttpActionAdapter.INSTANCE,
          "/",
          true, false, false, null);
    } else {
      Object authResult = securityLogic.perform(
          context,
          pac4jConfig,
          (J2EContext ctx, Collection<CommonProfile> profiles, Object... parameters) -> {
            if (profiles.isEmpty()) {
              LOGGER.warn("No profiles found after OIDC auth.");
              return null;
            } else {
              CommonProfile profile = profiles.iterator().next();
              Set<String> roles = profile.getRoles();

              if (roles == null || roles.isEmpty()) {
                return new AuthenticationResult(
                    profile.getId(),
                    authorizerName,
                    name,
                    null
                );
              } else {
                return new AuthenticationResult(
                    profile.getId(),
                    authorizerName,
                    name,
                    ImmutableMap.of("__druid_basic_security_roles", roles)
                );
              }
            }
          },
          J2ENopHttpActionAdapter.INSTANCE,
          null, null, null, null);

      if (authResult != null) {
        servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authResult);
        filterChain.doFilter(servletRequest, servletResponse);
      }
    }
  }

  @Override
  public void destroy()
  {
  }
}
