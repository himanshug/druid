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
import org.pac4j.http.client.direct.DirectBasicAuthClient;

public class Pac4jDirectHttpClientFactory implements Pac4jClientFactory
{
  private final Pac4jAuthenticatorrFactory pac4jAuthenticatorFactory;

  @Inject
  public Pac4jDirectHttpClientFactory(Pac4jAuthenticatorrFactory pac4jAuthenticatorFactory)
  {
    this.pac4jAuthenticatorFactory = pac4jAuthenticatorFactory;
  }

  @Override
  public Client getClient()
  {
    return new DirectBasicAuthClient(pac4jAuthenticatorFactory.getAuthenticator());
  }
}
