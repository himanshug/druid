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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;

public class Pac4jDruidModule implements DruidModule
{
  private static final String COMMON_KEY = "druid.auth.pac4j";

  private static final String OIDC_KEY = "oidc";
  private static final String LDAP_KEY = "ldap";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("Pac4jDruidSecurity").registerSubtypes(
            Pac4jAuthenticator.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, COMMON_KEY, Pac4jCommonConfig.class);
    JsonConfigProvider.bind(binder, StringUtils.format("%s.%s", COMMON_KEY, OIDC_KEY), OIDCConfig.class);
    JsonConfigProvider.bind(binder, StringUtils.format("%s.%s", COMMON_KEY, LDAP_KEY), LdapConfig.class);

    Jerseys.addResource(binder, Pac4jCallbackResource.class);

    PolyBind.createChoiceWithDefault(
        binder,
        StringUtils.format("%s.%s", COMMON_KEY, "client.type"),
        Key.get(Pac4jClientFactory.class),
        OIDC_KEY
    );

    PolyBind.optionBinder(binder, Key.get(Pac4jClientFactory.class))
            .addBinding(OIDC_KEY)
            .to(OidcClientFactory.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(Pac4jClientFactory.class))
            .addBinding("httpDirectBasicClient")
            .to(Pac4jDirectHttpClientFactory.class)
            .in(LazySingleton.class);

    PolyBind.createChoiceWithDefault(
        binder,
        StringUtils.format("%s.%s", COMMON_KEY, "authenticator.type"),
        Key.get(Pac4jAuthenticatorrFactory.class),
        "ldap"
    );

    PolyBind.optionBinder(binder, Key.get(Pac4jAuthenticatorrFactory.class))
            .addBinding(LDAP_KEY)
            .to(Pac4jLdapAuthenticatorFactory.class)
            .in(LazySingleton.class);
  }
}
