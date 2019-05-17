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

package org.apache.druid.k8s.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.regex.Pattern;

public class K8sDiscoveryConfig
{
  public static final Pattern K8S_RESOURCE_NAME_REGEX = Pattern.compile("[a-z0-9][a-z0-9-]*[a-z0-9]");

  @JsonProperty
  @Nonnull
  private final String clusterIdentifier;

  @JsonProperty
  private final String podNameEnvKey;

  @JsonProperty
  private final String podNamespaceEnvKey;

  @JsonProperty
  private final Duration leaseDuration;

  @JsonProperty
  private final Duration renewDeadline;

  @JsonProperty
  private final Duration retryPeriod;

  @JsonCreator
  public K8sDiscoveryConfig(
      @JsonProperty("clusterIdentifier") String clusterIdentifier,
      @JsonProperty("podNameEnvKey") String podNameEnvKey,
      @JsonProperty("podNamespaceEnvKey") String podNamespaceEnvKey,
      @JsonProperty("leaseDuration") Duration leaseDuration,
      @JsonProperty("renewDeadline") Duration renewDeadline,
      @JsonProperty("retryPeriod") Duration retryPeriod
  )
  {
    Preconditions.checkArgument(clusterIdentifier != null && !clusterIdentifier.isEmpty(), "null/empty clusterIdentifier");
    Preconditions.checkArgument(
        K8S_RESOURCE_NAME_REGEX.matcher(clusterIdentifier).matches(),
        "clusterIdentifier[%s] is used in k8s resource name and must match regex[%s]",
        clusterIdentifier,
        K8S_RESOURCE_NAME_REGEX.pattern()
    );
    this.clusterIdentifier = clusterIdentifier;

    this.podNameEnvKey = podNameEnvKey == null ? "POD_NAME" : podNameEnvKey;
    this.podNamespaceEnvKey = podNamespaceEnvKey == null ? "POD_NAMESPACE" : podNamespaceEnvKey;
    this.leaseDuration = leaseDuration == null ? Duration.ofMillis(90000) : leaseDuration;
    this.renewDeadline = renewDeadline == null ? Duration.ofMillis(60000) : renewDeadline;
    this.retryPeriod = retryPeriod == null ? Duration.ofMillis(5000) : retryPeriod;
  }

  @JsonProperty
  public String getClusterIdentifier()
  {
    return clusterIdentifier;
  }

  @JsonProperty
  public String getPodNameEnvKey()
  {
    return podNameEnvKey;
  }

  @JsonProperty
  public String getPodNamespaceEnvKey()
  {
    return podNamespaceEnvKey;
  }

  @JsonProperty
  public Duration getLeaseDuration()
  {
    return leaseDuration;
  }

  @JsonProperty
  public Duration getRenewDeadline()
  {
    return renewDeadline;
  }

  @JsonProperty
  public Duration getRetryPeriod()
  {
    return retryPeriod;
  }
}
