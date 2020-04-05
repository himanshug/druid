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
import org.ldaptive.ConnectionConfig;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.auth.FormatDnResolver;
import org.ldaptive.auth.PooledBindAuthenticationHandler;
import org.ldaptive.pool.BlockingConnectionPool;
import org.ldaptive.pool.IdlePruneStrategy;
import org.ldaptive.pool.PoolConfig;
import org.ldaptive.pool.PooledConnectionFactory;
import org.ldaptive.pool.SearchValidator;
import org.pac4j.core.credentials.authenticator.Authenticator;
import org.pac4j.ldap.profile.service.LdapProfileService;

import java.time.Duration;

public class Pac4jLdapAuthenticatorFactory implements Pac4jAuthenticatorrFactory
{
  private final LdapConfig ldapConfig;

  @Inject
  public Pac4jLdapAuthenticatorFactory(LdapConfig ldapConfig)
  {
    this.ldapConfig = ldapConfig;
  }

  @Override
  public Authenticator getAuthenticator()
  {
    FormatDnResolver dnResolver = new FormatDnResolver();
    dnResolver.setFormat("cn" + "=read-only-admin," + "ou=mathematicians,dc=example,dc=com");
    ConnectionConfig connectionConfig = new ConnectionConfig();
    connectionConfig.setConnectTimeout(Duration.ofMillis(500));
    connectionConfig.setResponseTimeout(Duration.ofMillis(1000));
    connectionConfig.setLdapUrl(ldapConfig.getUrl());
    DefaultConnectionFactory connectionFactory = new DefaultConnectionFactory();
    connectionFactory.setConnectionConfig(connectionConfig);
    PoolConfig poolConfig = new PoolConfig();
    poolConfig.setMinPoolSize(1);
    poolConfig.setMaxPoolSize(2);
    poolConfig.setValidateOnCheckOut(true);
    poolConfig.setValidateOnCheckIn(true);
    poolConfig.setValidatePeriodically(false);
    SearchValidator searchValidator = new SearchValidator();
    IdlePruneStrategy pruneStrategy = new IdlePruneStrategy();
    BlockingConnectionPool connectionPool = new BlockingConnectionPool();
    connectionPool.setPoolConfig(poolConfig);
    connectionPool.setBlockWaitTime(Duration.ofMillis(1000));
    connectionPool.setValidator(searchValidator);
    connectionPool.setPruneStrategy(pruneStrategy);
    connectionPool.setConnectionFactory(connectionFactory);
    connectionPool.initialize();
    PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
    pooledConnectionFactory.setConnectionPool(connectionPool);
    PooledBindAuthenticationHandler handler = new PooledBindAuthenticationHandler();
    handler.setConnectionFactory(pooledConnectionFactory);
    org.ldaptive.auth.Authenticator ldaptiveAuthenticator = new org.ldaptive.auth.Authenticator();
    ldaptiveAuthenticator.setDnResolver(dnResolver);
    ldaptiveAuthenticator.setAuthenticationHandler(handler);

    return new LdapProfileService(connectionFactory, ldaptiveAuthenticator, "sn", "ou=mathematicians,dc=example,dc=com");
  }
}
