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

import com.google.inject.Inject;
import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.NoParameterCallbackUrlResolver;
import org.pac4j.core.http.url.DefaultUrlResolver;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;

public class OidcClientFactory implements Pac4jClientFactory
{
  private final OIDCConfig oidcConfig;

  @Inject
  public OidcClientFactory(OIDCConfig oidcConfig)
  {
    this.oidcConfig = oidcConfig;
  }

  @Override
  public Client getClient()
  {
    OidcConfiguration oidcConf = new OidcConfiguration();
    oidcConf.setClientId(oidcConfig.getClientID());
    oidcConf.setSecret(oidcConfig.getClientSecret().getPassword());
    oidcConf.setDiscoveryURI(oidcConfig.getDiscoveryURI());
    oidcConf.setExpireSessionWithToken(true);
    oidcConf.setUseNonce(true);

    OidcClient oidcClient = new OidcClient(oidcConf);
    oidcClient.setUrlResolver(new DefaultUrlResolver(true));
    oidcClient.setCallbackUrlResolver(new NoParameterCallbackUrlResolver());

    return oidcClient;
  }
}
